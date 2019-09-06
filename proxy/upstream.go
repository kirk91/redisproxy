package proxy

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog"
)

const (
	slotNum = 16384

	ASKING      = "asking"
	MOVED       = "moved"
	ASK         = "ask"
	CLUSTERDOWN = "clusterdown"
)

var (
	// TODO: refine message
	upstreamExited = "upstream exited"
	backendExited  = "backend exited"
)

type createClientCall struct {
	done chan struct{}
	res  *client
	err  error
}

type upstream struct {
	hostsRwMu sync.RWMutex
	hosts     []string

	clients           atomic.Value // map[string]*client
	clientsMu         sync.Mutex
	createClientCalls sync.Map // map[string]*createClientCall

	slots               [slotNum]*redisHost
	slotsRefTriggerHook func() // only used to testing
	slotsRefCh          chan struct{}
	slotsLastUpdateTime time.Time

	quit chan struct{}
	done chan struct{}
}

func newUpstream(hosts []string) *upstream {
	u := &upstream{
		hosts:      hosts,
		slotsRefCh: make(chan struct{}, 1),
		quit:       make(chan struct{}),
		done:       make(chan struct{}),
	}
	u.clients.Store(make(map[string]*client))
	return u
}

func (u *upstream) Serve() {
	slotsLoopDone := make(chan struct{})
	go func() {
		u.loopRefreshSlots()
		close(slotsLoopDone)
	}()
	<-slotsLoopDone

	// stop all clients
	u.clientsMu.Lock()
	clients := u.loadClients()
	for _, c := range clients {
		c.Stop()
	}
	u.clientsMu.Unlock()
	close(u.done)
}

func (u *upstream) Stop() {
	close(u.quit)
	<-u.done
}

func (u *upstream) MakeRequest(key []byte, req *simpleRequest) {
	addr, err := u.chooseHost(key)
	if err != nil {
		req.SetResponse(newError(err.Error()))
		return
	}
	u.makeRequestToHost(addr, req)
}

func (u *upstream) chooseHost(key []byte) (string, error) {
	hash := crc16(hashtag(key))
	rhost := u.slots[hash&(slotNum-1)]
	if rhost != nil {
		return rhost.Addr, nil
	}

	// slot has no owner, choose one from the provided hosts randomly.
	host, err := u.randomHost()
	if err != nil {
		return "", err
	}
	return host, nil
}

func (u *upstream) makeRequestToHost(addr string, req *simpleRequest) {
	select {
	case <-u.quit:
		req.SetResponse(newError(upstreamExited))
		return
	default:
	}

	// TODO: validate addr
	c, err := u.getClient(addr)
	if err != nil {
		req.SetResponse(newError(err.Error()))
		return
	}
	// TODO: detect client status
	c.Send(req)
}

func (u *upstream) loadClients() map[string]*client {
	return u.clients.Load().(map[string]*client)
}

func (u *upstream) updateClients(clients map[string]*client) {
	u.clients.Store(clients)
}

func (u *upstream) getClient(addr string) (*client, error) {
	c, ok := u.loadClients()[addr]
	if ok {
		return c, nil
	}

	// NOTE: fail fast when the addr is unreachable
	v, loaded := u.createClientCalls.LoadOrStore(addr, &createClientCall{
		done: make(chan struct{}),
	})
	call := v.(*createClientCall)
	if loaded {
		<-call.done
		return call.res, call.err
	}
	c, err := u.createClient(addr)
	call.res, call.err = c, err
	close(call.done)
	return c, err
}

func (u *upstream) createClient(addr string) (*client, error) {
	u.clientsMu.Lock()
	defer u.clientsMu.Unlock()

	select {
	case <-u.quit:
		return nil, errors.New(backendExited)
	default:
	}
	c, ok := u.loadClients()[addr]
	if ok {
		return c, nil
	}

	// FIXME: use wrapper conn
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, err
	}
	c = newClient(conn)
	c.SetRedirectionCallback(u.handleRedirection)
	c.SetClusterDownCallabck(u.handleClusterDown)
	go func() {
		c.Start()
		u.removeClient(addr)
	}()
	u.addClientLocked(addr, c)
	return c, nil
}

func (u *upstream) addClientLocked(addr string, c *client) {
	clients := u.loadClients()
	clone := make(map[string]*client, len(clients)+1)
	for addr, c := range clients {
		clone[addr] = c
	}
	clone[addr] = c
	u.updateClients(clone)
}

func (u *upstream) removeClient(addr string) {
	u.clientsMu.Lock()
	defer u.clientsMu.Unlock()
	u.removeClientLocked(addr)
}

func (u *upstream) removeClientLocked(addr string) {
	clients := u.loadClients()
	clone := make(map[string]*client, len(clients)-1)
	for addr, c := range clients {
		clone[addr] = c
	}
	delete(clone, addr)
	u.updateClients(clone)
}

func (u *upstream) handleRedirection(req *simpleRequest, resp *RespValue) {
	parts := strings.Split(string(resp.Text), " ")
	// moved|ask slot addr, like 'moved 3999 127.0.0.1:6381'
	if len(parts) < 3 {
		req.SetResponse(resp)
		return
	}

	errPrefix := parts[0]
	hostAddr := parts[2]
	switch strings.ToLower(errPrefix) {
	case MOVED:
		u.makeRequestToHost(hostAddr, req)
	case ASK:
		askingReq := newSimpleRequest(newArray([]RespValue{
			*newBulkString(ASKING),
		}))
		u.makeRequestToHost(hostAddr, askingReq)
		u.makeRequestToHost(hostAddr, req)
	}
	u.triggerSlotsRefresh()
}

func (u *upstream) handleClusterDown(req *simpleRequest, resp *RespValue) {
	// Usually the cluster is able to recover itself after a CLUSTERDOWN
	// error, so try to request the new slots info.
	u.triggerSlotsRefresh()
	// TODO: add some retry
	req.SetResponse(resp)
}

func (u *upstream) triggerSlotsRefresh() {
	select {
	case u.slotsRefCh <- struct{}{}:
	default:
	}

	if u.slotsRefTriggerHook != nil {
		u.slotsRefTriggerHook()
	}
}

var (
	slotsRefFreq = time.Minute * 2
	// slots refresh minum rate is used to prevent execssive refresh reqeusts.
	slotsRefMinRate = 5 * time.Second
)

func (u *upstream) loopRefreshSlots() {
	u.triggerSlotsRefresh() // trigger slots refresh immediately
	for {
		select {
		case <-u.quit:
			return
		case <-time.After(slotsRefFreq):
		case <-u.slotsRefCh:
		}

		u.refreshSlots()

		t := time.NewTimer(slotsRefMinRate)
		select {
		case <-t.C:
		case <-u.quit:
			t.Stop()
			return
		}
	}
}

func (u *upstream) refreshSlots() {
	err := u.doSlotsRefresh()
	if err == nil {
		klog.V(4).Infof("refresh slots success")
		u.slotsLastUpdateTime = time.Now()
		return
	}

	klog.Warningf("fail to refresh slots: %v, will retry...", err)
	// retry it
	u.triggerSlotsRefresh()
	return
}

func (u *upstream) randomHost() (string, error) {
	u.hostsRwMu.RLock()
	defer u.hostsRwMu.RUnlock()
	if len(u.hosts) == 0 {
		return "", errors.New("no available host")
	}
	h := u.hosts[rand.Intn(len(u.hosts))]
	return h, nil
}

func (u *upstream) doSlotsRefresh() error {
	v := newArray([]RespValue{
		*newBulkString("cluster"),
		*newBulkString("nodes"),
	})
	req := newSimpleRequest(v)

	h, err := u.randomHost()
	if err != nil {
		return err
	}
	u.makeRequestToHost(h, req)

	// wait done
	req.Wait()
	resp := req.Response()
	if resp.Type == Error {
		return errors.New(string(resp.Text))
	}
	if resp.Type != BulkString {
		return errInvalidClusterNodes
	}
	hosts, err := parseClusterNodes(string(resp.Text))
	if err != nil {
		return err
	}

	// update slots
	for _, host := range hosts {
		for _, slot := range host.Slots {
			if slot < 0 || slot >= slotNum {
				continue
			}
			// NOTE: it's safe in x86-64 platform.
			u.slots[slot] = host
		}
	}
	return nil
}

type client struct {
	conn net.Conn
	enc  *encoder
	dec  *decoder

	pendingReqs    chan *simpleRequest
	processingReqs chan *simpleRequest

	onRedirection func(req *simpleRequest, resp *RespValue)
	onClusterDown func(req *simpleRequest, resp *RespValue)

	quitOnce sync.Once
	quit     chan struct{}
	done     chan struct{}
}

func newClient(conn net.Conn) *client {
	return &client{
		conn:           conn,
		enc:            newEncoder(conn, 4096),
		dec:            newDecoder(conn, 8192),
		pendingReqs:    make(chan *simpleRequest, 1024),
		processingReqs: make(chan *simpleRequest, 1024),
		quit:           make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func (c *client) SetRedirectionCallback(cb func(*simpleRequest, *RespValue)) {
	c.onRedirection = cb
}

func (c *client) SetClusterDownCallabck(cb func(*simpleRequest, *RespValue)) {
	c.onClusterDown = cb
}

func (c *client) Start() {
	writeDone := make(chan struct{})
	go func() {
		c.loopWrite()
		c.conn.Close()
		close(writeDone)
	}()

	c.loopRead()
	c.conn.Close()
	c.quitOnce.Do(func() {
		close(c.quit)
	})
	<-writeDone
	c.drainRequests()
	close(c.done)
}

func (c *client) Send(req *simpleRequest) {
	select {
	case <-c.quit:
		req.SetResponse(newError(backendExited))
	default:
		c.pendingReqs <- req
	}
}

func (c *client) loopWrite() {
	var (
		req *simpleRequest
		err error
	)
	for {
		select {
		case <-c.quit:
			return
		case req = <-c.pendingReqs:
		}

		err = c.enc.Encode(req.Body())
		if err != nil {
			goto FAIL
		}

		if len(c.pendingReqs) == 0 {
			if err = c.enc.Flush(); err != nil {
				goto FAIL
			}
		}
		c.processingReqs <- req
	}

FAIL:
	// req and error must not be nil
	req.SetResponse(newError(err.Error()))
	klog.Warningf("loop write exit: %v", err)
}

func (c *client) loopRead() {
	for {
		resp, err := c.dec.Decode()
		if err != nil {
			if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
				klog.Warningf("loop read exit: %v", err)
			}
			return
		}

		req := <-c.processingReqs
		c.handleResp(req, resp)
	}
}

func (c *client) handleResp(req *simpleRequest, v *RespValue) {
	if v.Type != Error {
		req.SetResponse(v)
		return
	}

	parts := strings.Split(string(v.Text), " ")
	switch errPrefix := parts[0]; {
	case strings.EqualFold(errPrefix, MOVED),
		strings.EqualFold(errPrefix, ASK):
		if c.onRedirection != nil {
			c.onRedirection(req, v)
			return
		}
	case strings.EqualFold(errPrefix, CLUSTERDOWN):
		if c.onClusterDown != nil {
			c.onClusterDown(req, v)
			return
		}
	}

	// set error as response
	req.SetResponse(v)
}

func (c *client) drainRequests() {
	for {
		select {
		case req := <-c.pendingReqs:
			req.SetResponse(newError(backendExited))
		case req := <-c.processingReqs:
			req.SetResponse(newError(backendExited))
		default:
			return
		}
	}
}

func (c *client) Stop() {
	c.quitOnce.Do(func() {
		close(c.quit)
	})
	c.conn.Close()
	<-c.done
}
