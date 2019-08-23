package main

import (
	"net"
	"testing"
	"time"
)

func TestSessionClose(t *testing.T) {
	conn, _ := net.Pipe()
	s := newSession(conn, nil)
	done := make(chan struct{})
	go func() {
		s.Serve()
		close(done)
	}()

	time.Sleep(time.Millisecond * 500)
	s.Close()
	<-done
}

func TestSessionReadError(t *testing.T) {
	cconn, sconn := net.Pipe()
	s := newSession(cconn, nil)
	done := make(chan struct{})
	go func() {
		s.Serve()
		close(done)
	}()

	time.AfterFunc(time.Millisecond*100, func() {
		sconn.Close()
	})
	<-done
}

func TestSessionWriteError(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	conn, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Error(err)
		return
	}
	defer conn.Close()

	s := newSession(conn, nil)
	done := make(chan struct{})
	go func() {
		s.Serve()
		close(done)
	}()

	time.AfterFunc(time.Millisecond*100, func() {
		conn.(*net.TCPConn).CloseWrite()

		// make a rawRequest
		sconn, _ := l.Accept()
		sconn.Write(encode(newArray([]RespValue{
			*newBulkString("get"),
			*newBulkString("a"),
		})))
		<-done
		sconn.Close()
	})
	<-done
}
