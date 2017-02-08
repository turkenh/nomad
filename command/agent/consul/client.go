// +build ignore
package consul

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/nomad/nomad/structs"
)

type Executor interface {
	Exec(ctx context.Context, cmd string, args []string) error
}

type Client struct {
	retryInterval time.Duration
	syncInterval  time.Duration

	shutdownCh chan struct{}
	syncCh     chan struct{}

	// services and checks to be registered
	regServices map[string]*api.AgentServiceRegisration
	regChecks   map[string]*api.AgentServiceCheck

	// services and checks to be unregisterd
	deregServices map[string]string
	deregChecks   map[string]string

	// regLock must be held while accessing reg and dereg maps
	regLock sync.Mutex
}

func (c *Client) Run() {
	timer := time.NewTimer(0)
	defer timer.Stop()

DISCO:
	for {
		select {
		case <-c.shutdownCh:
			return
		case <-timer.C:
			if ok := c.connect(); ok {
				break DISCO
			}
			timer.Reset(c.retryInterval)
		}
	}

	timer.Reset(0)
	for {
		select {
		case <-c.syncCh:
			timer.Reset(0)
		case <-timer.C:
			if err := c.sync(); err != nil {
				timer.Reset(c.retryInterval)
				continue
			}
			timer.Reset(c.syncInterval)
		case <-c.shutdownCh:
			return
		}
	}
}

func (c *Client) RegisterTask(allocID string, task *structs.Task, exec Executor) {
	regs := make([]*api.AgentServiceRegistration, len(task.Services))
	checks := make([]*api.AgentServiceCheck, 0, len(task.Services)) // just guess at size

	for i, service := range task.Services {
		id := makeServiceKey(allocID, task.Name, service.Name)
		host, port := task.AddrFinder(service.PortLabel)
		regs[i] = &api.AgentServiceRegistration{
			ID:      id,
			Name:    service.Name,
			Tags:    service.Tags,
			Address: addr,
			Port:    port,
		}

		for _, check := range service.Checks {
			if check.Type == structs.ServiceCheckScript {
				//TODO async start check
				c.startCheck(check, exec)
				continue
			}
			host, port := serviceReg.Address, serviceReg.Port
			if check.PortLabel != "" {
				host, port = task.FindHostAndPortFor(check.PortLabel)
			}
			checks = append(checks, createCheckReg(service.ID, check, host, port))
		}

	}

	// Now add them to the registration fields
	c.prepareRegistrations(regs, checks)
}

func (c *Client) prepareRegistrations(regs []*api.AgentServiceRegistration, checks []*api.AgentServiceCheck) {
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
}

func (c *Client) AddServices(domain ServiceDomain, services []*structs.Service, advertise string, exec Executor) error {
	host, port, err := net.SplitHostPort
	for _, service := range services {
		id := makeServiceKey(domain, service)
		serviceReg := consul.AgentServiceRegistration{
			ID:   id,
			Name: service.Name,
			Tags: service.Tags,
		}

		//TODO addrFinder :( Pass in to method? Pass in addr/port as args?
		host, port := c.addrFinder(service.PortLabel)
		if host != "" {
			srv.Address = host
		}

		if port != 0 {
			srv.Port = port
		}

		// Register the checks for this service
		for _, check := range service.Checks {
			//TODO
			if check.Type == structs.ServiceCheckScript {
				c.startCheck(check, exec)
			}

		}
	}

	//TODO Actually perform registration
	//TODO Lock and add to map
	//XXX  This approach will *only* work if reconciliation is blocked
	//since if we register synchronously before adding to syncer's map
	//there's a race where reconciliation might run and try to remove the
	//created entries
}

func (c *Client) startCheck(check *structs.ServiceCheck, exec Executor) {
	panic("TODO - run sevice check script in a goroutine")
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

// createCheckReg creates a Check that can be registered with Consul.
//
// Only supports HTTP(S) and TCP checks. Script checks must be handled
// externally.
func createCheckReg(serviceID string, check *structs.ServiceCheck, host string, port int) (*consul.AgentCheckRegistration, error) {
	chkReg := consul.AgentCheckRegistration{
		ID:        check.Hash(serviceID),
		Name:      check.Name,
		ServiceID: serviceID,
		Status:    check.InitialStatus,
		Timeout:   check.Timeout.String(),
		Interval:  check.Interval.String(),
	}

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
		panic("TODO - handle outside this function")
	default:
		return nil, fmt.Errorf("check type %+q not valid", check.Type)
	}
	return &chkReg, nil
}
