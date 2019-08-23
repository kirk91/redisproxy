package main

import (
	"io"
	"net"
	"sync"

	"k8s.io/klog"
)

type reqHandleFunc func(req *rawRequest)

type session struct {
	conn net.Conn
	dec  *decoder
	enc  *encoder

	reqHandleFn    reqHandleFunc
	processingReqs chan *rawRequest

	quitOnce sync.Once
	quit     chan struct{}
	done     chan struct{}
}

func newSession(conn net.Conn, reqHandleFn reqHandleFunc) *session {
	return &session{
		conn:           conn,
		enc:            newEncoder(conn, 8192),
		dec:            newDecoder(conn, 4096),
		processingReqs: make(chan *rawRequest, 32),
		quit:           make(chan struct{}),
		done:           make(chan struct{}),
	}
}

func (s *session) Serve() {
	writeDone := make(chan struct{})
	go func() {
		s.loopWrite()
		s.conn.Close()
		close(writeDone)
	}()

	s.loopRead()
	s.conn.Close()
	s.quitOnce.Do(func() {
		close(s.quit)
	})
	<-writeDone
	close(s.done)
}

func (s *session) Close() {
	s.quitOnce.Do(func() {
		close(s.quit)
	})
	s.conn.Close()
	<-s.done
}

func (s *session) loopRead() {
	for {
		v, err := s.dec.Decode()
		if err != nil {
			if err != io.EOF {
				klog.Warningf("loop read exit: %v", err)
			}
			return
		}

		req := newRawRequest(v)
		s.handleRequest(req)
		s.processingReqs <- req
	}
}

func (s *session) handleRequest(req *rawRequest) {
	if s.reqHandleFn == nil {
		req.SetResponse(newError("no registered handler for request"))
		return
	}
	s.reqHandleFn(req)
}

func (s *session) loopWrite() {
	var (
		req *rawRequest
		err error
	)
	for {
		select {
		case <-s.quit:
			return
		case req = <-s.processingReqs:
		}

		req.Wait()
		// TODO(kirk91): abstract response
		resp := req.Response()
		if err = s.enc.Encode(resp); err != nil {
			goto FAIL
		}

		// there are some processing requests, we could flush later to
		// reduce the amount of write syscalls.
		if len(s.processingReqs) != 0 {
			continue
		}
		// flush the buffer data to the underlying connection.
		if err = s.enc.Flush(); err != nil {
			goto FAIL
		}
	}

FAIL:
	klog.Warningf("loop write exit: %v", err)
}
