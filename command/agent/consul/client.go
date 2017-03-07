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
	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/nomad/structs/config"
)

var mark = struct{}{}

//TODO rename?!
type ScriptExecutor interface {
	Exec(ctx context.Context, cmd string, args []string) ([]byte, int, error)
}

//TODO rename?!
type Client struct {
	//TODO switch to interface for testing
	client        *api.Client
	logger        *log.Logger
	retryInterval time.Duration
	syncInterval  time.Duration

	// runningCh is closed when the main Run loop exits
	runningCh chan struct{}

	// shutdownCh is closed when the client should shutdown
	shutdownCh chan struct{}

	// syncCh triggers a sync in the main Run loop
	syncCh chan struct{}

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

	// Registered agent services and checks
	agentServices map[string]struct{}
	agentChecks   map[string]struct{}

	// agentLock must be held while accessing agent maps
	agentLock sync.Mutex
}

func NewClient(consulConfig *config.ConsulConfig, logger *log.Logger) (*Client, error) {
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
		runningCh:     make(chan struct{}),
		shutdownCh:    make(chan struct{}),
		syncCh:        make(chan struct{}, 1),
		regServices:   make(map[string]*api.AgentServiceRegistration),
		regChecks:     make(map[string]*api.AgentCheckRegistration),
		deregServices: make(map[string]struct{}),
		deregChecks:   make(map[string]struct{}),
		scriptChecks:  make(map[string]func()),
		agentServices: make(map[string]struct{}, 8),
		agentChecks:   make(map[string]struct{}, 8),
	}, nil
}

// Run the Consul main loop which performs registrations and deregistrations
// against Consul. It should be called exactly once.
func (c *Client) Run() {
	defer close(c.runningCh)
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
				c.logger.Printf("[WARN] consul: failed to update consul: %v", err)
				//TODO Log and jitter/backoff
				timer.Reset(c.retryInterval)
			}
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) forceSync() {
	select {
	case c.syncCh <- mark:
	default:
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

	regServiceN, regCheckN, deregServiceN, deregCheckN := len(regServices), len(regChecks), len(deregServices), len(deregChecks)

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

	c.logger.Printf("[DEBUG] consul: registered %d services / %d checks; deregisterd %d services / %d checks", regServiceN, regCheckN, deregServiceN, deregCheckN)
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

// DiscoverServers tries to discover Nomad servers in a region. The results are
// ordered with servers in the same datacenter first.
//
// If the context is cancelled nil is returned.
func (c *Client) DiscoverServers(ctx context.Context, region, dc string) <-chan []string {
	resp := make(chan []string, 1)
	go func() {
		panic("TODO - actual server discovery")
	}()
	return resp
}

// RegisterAgent registers Nomad agents (client or server). Script checks are
// not supported and will return an error. Registration is asynchronous.
//
// Agents will be deregistered when Shutdown is called.
func (c *Client) RegisterAgent(role string, services []*structs.Service) error {
	regs := make([]*api.AgentServiceRegistration, len(services))
	checks := make([]*api.AgentCheckRegistration, 0, len(services))

	for i, service := range services {
		id := makeAgentServiceID(role, service)
		host, rawport, err := net.SplitHostPort(service.PortLabel)
		if err != nil {
			return fmt.Errorf("error parsing port label %q from service %q: %v", service.PortLabel, service.Name, err)
		}
		port, err := strconv.Atoi(rawport)
		if err != nil {
			return fmt.Errorf("error parsing port %q from service %q: %v", rawport, service.Name, err)
		}
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
				return fmt.Errorf("service %q contains invalid check: agent checks do not support scripts", service.Name)
			}
			checkHost, checkPort := serviceReg.Address, serviceReg.Port
			if check.PortLabel != "" {
				host, rawport, err := net.SplitHostPort(check.PortLabel)
				if err != nil {
					return fmt.Errorf("error parsing port label %q from check %q: %v", service.PortLabel, check.Name, err)
				}
				port, err := strconv.Atoi(rawport)
				if err != nil {
					return fmt.Errorf("error parsing port %q from check %q: %v", rawport, check.Name, err)
				}
				checkHost, checkPort = host, port
			}
			checkReg, err := createCheckReg(id, checkID, check, checkHost, checkPort)
			if err != nil {
				return fmt.Errorf("failed to add check %q: %v", check.Name, err)
			}
			checks = append(checks, checkReg)
		}
	}

	// Now add them to the registration queue
	c.enqueueRegs(regs, checks, nil)

	// Record IDs for deregistering on shutdown
	c.agentLock.Lock()
	for _, s := range regs {
		c.agentServices[s.ID] = mark
	}
	for _, ch := range checks {
		c.agentChecks[ch.ID] = mark
	}
	c.agentLock.Unlock()
	return nil
}

// RegisterTask with Consul. Adds all sevice entries and checks to Consul. If
// exec is nil and a script check exists an error is returned.
//
// Actual communication with Consul is done asynchrously (see Run).
func (c *Client) RegisterTask(allocID string, task *structs.Task, exec ScriptExecutor) error {
	regs := make([]*api.AgentServiceRegistration, len(task.Services))
	checks := make([]*api.AgentCheckRegistration, 0, len(task.Services)*2) // just guess at size
	scriptChecks := map[string]*scriptCheck{}

	for i, service := range task.Services {
		id := makeTaskServiceID(allocID, task.Name, service)
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
				if exec == nil {
					return fmt.Errorf("driver %q doesn't support script checks", task.Driver)
				}
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

	// Now add them to the registration queue
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
		id := makeTaskServiceID(allocID, task.Name, service)
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
	c.regLock.Unlock()

	c.forceSync()
}

func (c *Client) enqueueDeregs(deregs []string, checks []string) {
	c.regLock.Lock()
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
	c.regLock.Unlock()

	c.forceSync()
}

func (c *Client) hasShutdown() bool {
	select {
	case <-c.shutdownCh:
		return true
	default:
		return false
	}
}

// Shutdown the Consul client. Deregister agent from Consul.
func (c *Client) Shutdown() error {
	if c.hasShutdown() {
		return nil
	}
	close(c.shutdownCh)

	var mErr multierror.Error

	// Deregister agent services and checks
	c.agentLock.Lock()
	for id := range c.agentServices {
		if err := c.client.Agent().ServiceDeregister(id); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	}

	// Deregister Checks
	for id := range c.agentChecks {
		if err := c.client.Agent().CheckDeregister(id); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	}
	c.agentLock.Unlock()

	// Wait for Run to finish any outstanding sync() calls and exit
	select {
	case <-c.runningCh:
		// sync one last time to ensure all enqueued operations are applied
		if err := c.sync(); err != nil {
			mErr.Errors = append(mErr.Errors, err)
		}
	case <-time.After(time.Minute):
		// Don't wait forever though
		c.logger.Printf("[WARN] consul: timed out waiting for Consul operations to complete")
	}
	return mErr.ErrorOrNil()
}

// makeAgentServiceID creates a unique ID for identifying an agent service in
// Consul.
//
// Agent service IDs are of the form:
//
//	{nomadServicePrefix}-{ROLE}-{Service.Name}-{Service.Tags...}
//	Example Server ID: _nomad-server-nomad-serf
//	Example Client ID: _nomad-client-nomad-client-http
//
func makeAgentServiceID(role string, service *structs.Service) string {
	parts := make([]string, len(service.Tags)+3)
	parts[0] = nomadServicePrefix
	parts[1] = role
	parts[2] = service.Name
	copy(parts[3:], service.Tags)
	return strings.Join(parts, "-")
}

// makeTaskServiceID creates a unique ID for identifying a task service in
// Consul.
//
// Task service IDs are of the form:
//
//	{nomadServicePrefix}-executor-{ALLOC_ID}-{Service.Name}-{Service.Tags...}
//	Example Service ID: _nomad-executor-1234-echo-http-tag1-tag2-tag3
//
func makeTaskServiceID(allocID, taskName string, service *structs.Service) string {
	parts := make([]string, len(service.Tags)+5)
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
