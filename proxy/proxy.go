package proxy

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/kirk91/stats"
	"k8s.io/klog"
)

type Config struct {
	Bind  int
	Hosts []string
}

type proxy struct {
	mu       sync.Mutex
	cfg      *Config
	stats    *stats.Scope
	u        *upstream
	cmdHdlrs map[string]*commandHandler

	ln       net.Listener
	sessions map[*session]struct{}

	wg   sync.WaitGroup
	quit chan struct{}
}

func New(cfg *Config, scope *stats.Scope) (*proxy, error) {
	p := &proxy{
		cfg:      cfg,
		stats:    scope,
		cmdHdlrs: make(map[string]*commandHandler),
		quit:     make(chan struct{}),
	}
	p.u = newUpstream(cfg.Hosts)
	p.initCommandHandlers()
	return p, nil
}

func (p *proxy) initCommandHandlers() {
	scope := p.stats.NewChild("cmd")
	for _, cmd := range simpleCommands {
		p.addCmdHandler(scope, cmd, handleSimpleCommand)
	}

	for _, cmd := range sumResultCommands {
		p.addCmdHandler(scope, cmd, handleSumResultCommand)
	}

	p.addCmdHandler(scope, "eval", handleEval)
	p.addCmdHandler(scope, "mset", handleMSet)
	p.addCmdHandler(scope, "mget", handleMGet)

	p.addCmdHandler(scope, "ping", handlePing)
	p.addCmdHandler(scope, "quit", handleQuit)
	p.addCmdHandler(scope, "info", handleInfo)
	p.addCmdHandler(scope, "time", handleTime)
	p.addCmdHandler(scope, "select", handleSelect)
}

func (p *proxy) addCmdHandler(scope *stats.Scope, cmd string, fn commandHandleFunc) {
	hdlr := &commandHandler{
		stats:  newCommandStats(scope, cmd),
		handle: fn,
	}
	p.cmdHdlrs[cmd] = hdlr
}

func (p *proxy) findCmdHandler(cmd string) (*commandHandler, bool) {
	hdlr, ok := p.cmdHdlrs[strings.ToLower(cmd)]
	return hdlr, ok
}

func (p *proxy) ListenAndServe() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", p.cfg.Bind))
	if err != nil {
		return err
	}
	p.ln = ln

	// start upstream
	p.wg.Add(1)
	go func() {
		p.u.Serve()
		p.wg.Done()
	}()

	var tempDelay time.Duration
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				klog.Warningf("accept failed: %v; retrying in %s", err, tempDelay)
				timer := time.NewTimer(tempDelay)
				select {
				case <-timer.C:
				case <-p.quit:
					timer.Stop()
					return nil
				}
				continue
			}

			select {
			case <-p.quit:
				return nil
			default:
			}
			return err
		}

		p.wg.Add(1)
		go func(conn net.Conn) {
			p.handleRawConn(conn)
			p.wg.Done()
		}(conn)
	}
}

func (p *proxy) Stop() error {
	close(p.quit)
	p.mu.Lock()
	sessions := p.sessions
	p.sessions = nil
	p.mu.Unlock()

	if p.ln != nil {
		p.ln.Close()
	}
	for s := range sessions {
		s.Close()
	}
	p.u.Stop()
	return nil
}

func (p *proxy) handleRawConn(conn net.Conn) {
	// TODO: use conn wrapper to record read&write metrics
	t := time.Now()
	laddr, raddr := conn.LocalAddr(), conn.RemoteAddr()
	klog.V(4).Infof("%s -> %s created", raddr, laddr)
	defer func() {
		klog.V(4).Infof("%s -> %s finished, duration: %s", laddr, raddr, time.Since(t))
	}()

	s := newSession(conn, p.handleRequest)
	if !p.addSession(s) {
		s.Close()
		return
	}

	s.Serve()
	p.removeSession(s)
}

func (p *proxy) addSession(s *session) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	// proxy is quiting
	if p.sessions == nil {
		return false
	}
	p.sessions[s] = struct{}{}
	return true
}

func (p *proxy) removeSession(s *session) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sessions == nil {
		return
	}
	delete(p.sessions, s)
}

func (p *proxy) handleRequest(req *rawRequest) {
	// check
	if !req.IsValid() {
		req.SetResponse(newError(invalidRequest))
		return
	}

	// pre process with filters

	// find corresponding _handler
	cmd := string(req.Body().Array[0].Text)
	hdlr, ok := p.findCmdHandler(cmd)
	if !ok {
		// unsupported command
		req.SetResponse(newError(fmt.Sprintf("unsupported command %s", cmd)))
		return
	}

	cmdStats := hdlr.stats
	cmdStats.Total.Inc()
	req.RegisterHook(func(req *rawRequest) {
		switch req.Response().Type {
		case Error:
			cmdStats.Error.Inc()
		default:
			cmdStats.Success.Inc()
		}
		latency := uint64(req.Duration() / time.Microsecond)
		cmdStats.LatencyMicros.Record(latency)
	})
	hdlr.handle(p.u, req)
}
