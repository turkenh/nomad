package consul

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/nomad/structs/config"
)

var mark = struct{}{}

//TODO rename?!
type Executor interface {
	Exec(ctx context.Context, cmd string, args []string) ([]byte, int, error)
}

//TODO rename?!
type Client struct {
	//TODO switch to interface for testing
	client        *api.Client
	logger        *log.Logger
	retryInterval time.Duration
	syncInterval  time.Duration

	shutdownCh chan struct{}
	syncCh     chan struct{}

	// services and checks to be registered
	regServices map[string]*api.AgentServiceRegistration
	regChecks   map[string]*api.AgentCheckRegistration

	// services and checks to be unregisterd
	deregServices map[string]struct{}
	deregChecks   map[string]struct{}

	// scriptChecks currently running and their cancel func
	scriptChecks map[string]func()

	// regLock must be held while accessing reg and dereg maps
	regLock sync.Mutex
}

func NewClient(consulConfig *config.ConsulConfig, shutdownCh chan struct{}, logger *log.Logger) (*Client, error) {
	apiConf, err := consulConfig.ApiConfig()
	if err != nil {
		return nil, err
	}
	client, err := api.NewClient(apiConf)
	if err != nil {
		return nil, err
	}
	return &Client{
		client:        client,
		logger:        logger,
		retryInterval: defaultSyncInterval, //TODO what should this default to?!
		syncInterval:  defaultSyncInterval,
		shutdownCh:    shutdownCh,
		syncCh:        make(chan struct{}, 1),
		regServices:   make(map[string]*api.AgentServiceRegistration),
		regChecks:     make(map[string]*api.AgentCheckRegistration),
		deregServices: make(map[string]struct{}),
		deregChecks:   make(map[string]struct{}),
		scriptChecks:  make(map[string]func()),
	}, nil
}

func (c *Client) Run() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	// Drain the initial tick so we don't sync until instructed
	<-timer.C

	for {
		select {
		case <-c.syncCh:
			timer.Reset(0)
		case <-timer.C:
			if err := c.sync(); err != nil {
				//TODO Log and jitter/backoff
				timer.Reset(c.retryInterval)
				continue
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) sync() error {
	// Shallow copy and reset the pending operations fields
	c.regLock.Lock()
	regServices := make(map[string]*api.AgentServiceRegistration, len(c.regServices))
	for k, v := range c.regServices {
		regServices[k] = v
	}
	c.regServices = map[string]*api.AgentServiceRegistration{}

	regChecks := make(map[string]*api.AgentCheckRegistration, len(c.regChecks))
	for k, v := range c.regChecks {
		regChecks[k] = v
	}
	c.regChecks = map[string]*api.AgentCheckRegistration{}

	deregServices := make(map[string]struct{}, len(c.deregServices))
	for k := range c.deregServices {
		deregServices[k] = mark
	}
	c.deregServices = map[string]struct{}{}

	deregChecks := make(map[string]struct{}, len(c.deregChecks))
	for k := range c.deregChecks {
		deregChecks[k] = mark
	}
	c.deregChecks = map[string]struct{}{}
	c.regLock.Unlock()

	var err error

	// Register Services
	for id, service := range regServices {
		if err = c.client.Agent().ServiceRegister(service); err != nil {
			goto ERROR
		}
		delete(regServices, id)
	}

	// Register Checks
	for id, check := range regChecks {
		if err = c.client.Agent().CheckRegister(check); err != nil {
			goto ERROR
		}
		delete(regChecks, id)
	}

	// Deregister Services
	for id := range deregServices {
		if err = c.client.Agent().ServiceDeregister(id); err != nil {
			goto ERROR
		}
		delete(deregServices, id)
	}

	// Deregister Checks
	for id := range deregChecks {
		if err = c.client.Agent().CheckDeregister(id); err != nil {
			goto ERROR
		}
		delete(deregChecks, id)
	}
	return nil

	//TODO Labels and gotos are nasty; move to a function?
ERROR:
	// An error occurred, repopulate the operation maps omitting any keys
	// that have been updated while sync() ran.
	c.regLock.Lock()
	for id, service := range regServices {
		if _, ok := c.regServices[id]; ok {
			continue
		}
		if _, ok := c.deregServices[id]; ok {
			continue
		}
		c.regServices[id] = service
	}
	for id, check := range regChecks {
		if _, ok := c.regChecks[id]; ok {
			continue
		}
		if _, ok := c.deregChecks[id]; ok {
			continue
		}
		c.regChecks[id] = check
	}
	for id, _ := range deregServices {
		if _, ok := c.regServices[id]; ok {
			continue
		}
		c.deregServices[id] = mark
	}
	for id, _ := range deregChecks {
		if _, ok := c.regChecks[id]; ok {
			continue
		}
		c.deregChecks[id] = mark
	}
	c.regLock.Unlock()
	return err
}

// RegisterTask with Consul. Adds all sevice entries and checks to Consul.
//
// Actual communication with Consul is done asynchrously (see Run).
func (c *Client) RegisterTask(allocID string, task *structs.Task, exec Executor) error {
	regs := make([]*api.AgentServiceRegistration, len(task.Services))
	checks := make([]*api.AgentCheckRegistration, 0, len(task.Services)*2) // just guess at size
	scriptChecks := map[string]*scriptCheck{}

	for i, service := range task.Services {
		id := makeServiceKey(allocID, task.Name, service)
		host, port := task.FindHostAndPortFor(service.PortLabel)
		serviceReg := &api.AgentServiceRegistration{
			ID:      id,
			Name:    service.Name,
			Tags:    service.Tags,
			Address: host,
			Port:    port,
		}
		regs[i] = serviceReg

		for _, check := range service.Checks {
			checkID := createCheckID(id, check)
			if check.Type == structs.ServiceCheckScript {
				scriptChecks[checkID] = newScriptCheck(checkID, check, exec, c.client.Agent(), c.logger, c.shutdownCh)
			}
			host, port := serviceReg.Address, serviceReg.Port
			if check.PortLabel != "" {
				host, port = task.FindHostAndPortFor(check.PortLabel)
			}
			checkReg, err := createCheckReg(id, checkID, check, host, port)
			if err != nil {
				return fmt.Errorf("failed to add check %q: %v", check.Name, err)
			}
			checks = append(checks, checkReg)
		}

	}

	// Now add them to the registration fields
	c.enqueueRegs(regs, checks, scriptChecks)
	return nil
}

// DeregisterTask from Consul. Removes all service entries and checks.
//
// Actual communication with Consul is done asynchrously (see Run).
func (c *Client) RemoveTask(allocID string, task *structs.Task) {
	deregs := make([]string, len(task.Services))
	checks := make([]string, 0, len(task.Services)*2) // just guess at size

	for i, service := range task.Services {
		id := makeServiceKey(allocID, task.Name, service)
		deregs[i] = id

		for _, check := range service.Checks {
			checkID := createCheckID(id, check)
			if check.Type == structs.ServiceCheckScript {
				// Unlike registeration, deregistration can't
				// be interrupted due to errors so we can
				// cancel script checks as we go instead of
				// doing it when deregs are enqueued
				c.regLock.Lock()
				if cancel, ok := c.scriptChecks[checkID]; ok {
					cancel()
				}
				c.regLock.Unlock()
				continue
			}
			checks = append(checks, checkID)
		}
	}

	// Now add them to the deregistration fields; main Run loop will update
	c.enqueueDeregs(deregs, checks)
}

func (c *Client) enqueueRegs(regs []*api.AgentServiceRegistration, checks []*api.AgentCheckRegistration, scriptChecks map[string]*scriptCheck) {
	c.regLock.Lock()
	defer c.regLock.Unlock()
	for _, reg := range regs {
		// Add reg
		c.regServices[reg.ID] = reg
		// Make sure it's not being removed
		delete(c.deregServices, reg.ID)
	}
	for _, check := range checks {
		// Add check
		c.regChecks[check.ID] = check
		// Make sure it's not being removed
		delete(c.deregChecks, check.ID)
	}
	// Start script checks and retain their cancel funcs
	for checkID, check := range scriptChecks {
		c.scriptChecks[checkID] = check.run()
	}
}

func (c *Client) enqueueDeregs(deregs []string, checks []string) {
	c.regLock.Lock()
	defer c.regLock.Unlock()
	for _, dereg := range deregs {
		// Add dereg
		c.deregServices[dereg] = mark
		// Make sure it's not being added
		delete(c.regServices, dereg)
	}
	for _, check := range checks {
		// Add check for removal
		c.deregChecks[check] = mark
		// Make sure it's not being added
		delete(c.regChecks, check)
	}
}

func (c *Client) hasShutdown() bool {
	select {
	case <-c.shutdownCh:
		return true
	default:
		return false
	}
}

// makeServiceKey creates a unique ID for identifying a service in Consul.
//
// Service keys are of the form:
//
//	{nomadServicePrefix}-executor-{ALLOC_ID}-{Service.Name}-{Service.Tags...}
//	Example Service Key: _nomad-executor-1234-echo-http-tag1-tag2-tag3
//
func makeServiceKey(allocID, taskName string, service *structs.Service) string {
	parts := make([]string, len(service.Tags)+3)
	parts[0] = nomadServicePrefix
	parts[1] = "executor"
	parts[2] = allocID
	parts[3] = taskName
	parts[4] = service.Name
	copy(parts[5:], service.Tags)
	return strings.Join(parts, "-")
}

// createCheckID creates a unique ID for a check.
func createCheckID(serviceID string, check *structs.ServiceCheck) string {
	return check.Hash(serviceID)
}

// createCheckReg creates a Check that can be registered with Consul.
//
// Only supports HTTP(S) and TCP checks. Script checks must be handled
// externally.
func createCheckReg(serviceID, checkID string, check *structs.ServiceCheck, host string, port int) (*api.AgentCheckRegistration, error) {
	chkReg := api.AgentCheckRegistration{
		ID:        checkID,
		Name:      check.Name,
		ServiceID: serviceID,
	}
	chkReg.Status = check.InitialStatus
	chkReg.Timeout = check.Timeout.String()
	chkReg.Interval = check.Interval.String()

	switch check.Type {
	case structs.ServiceCheckHTTP:
		if check.Protocol == "" {
			check.Protocol = "http"
		}
		base := url.URL{
			Scheme: check.Protocol,
			Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		}
		relative, err := url.Parse(check.Path)
		if err != nil {
			return nil, err
		}
		url := base.ResolveReference(relative)
		chkReg.HTTP = url.String()
	case structs.ServiceCheckTCP:
		chkReg.TCP = net.JoinHostPort(host, strconv.Itoa(port))
	case structs.ServiceCheckScript:
		chkReg.TTL = (check.Interval + ttlCheckBuffer).String()
	default:
		return nil, fmt.Errorf("check type %+q not valid", check.Type)
	}
	return &chkReg, nil
}
