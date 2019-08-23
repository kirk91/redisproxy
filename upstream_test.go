package main

import (
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpstreamClientReadError(t *testing.T) {
	cconn, sconn := net.Pipe()
	c := newClient(cconn)
	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()
	time.AfterFunc(time.Millisecond*100, func() {
		sconn.Close()
	})
	<-done
}

func TestUpstreamClientWriteError(t *testing.T) {
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

	tcpConn := conn.(*net.TCPConn)
	c := newClient(tcpConn)
	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()
	time.AfterFunc(time.Millisecond*100, func() {
		tcpConn.CloseWrite()
		c.Send(newSimpleRequest(newSimpleString("ping")))
	})
	<-done
}

func TestUpstreamClientStop(t *testing.T) {
	cconn, _ := net.Pipe()
	c := newClient(cconn)
	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()
	time.AfterFunc(time.Millisecond*100, func() {
		c.Stop()
	})
	<-done
}

func TestUpstreamClientDrainRequests(t *testing.T) {
	cconn, _ := net.Pipe()
	c := newClient(cconn)
	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()
	time.AfterFunc(time.Millisecond*100, c.Stop)

	var reqs []*simpleRequest
loop:
	for {
		select {
		case <-done:
			break loop
		default:
		}
		req := newSimpleRequest(newSimpleString("ping"))
		c.Send(req)
		reqs = append(reqs, req)
	}

	// assert
	for _, req := range reqs {
		req.Wait()
	}
}

func TestUpstreamClientHandleRedirection(t *testing.T) {
	cconn, sconn := net.Pipe()
	c := newClient(cconn)
	done := make(chan struct{})
	go func() {
		c.Start()
		close(done)
	}()
	defer func() {
		c.Stop()
		<-done
	}()

	c.SetRedirectionCallback(func(req *simpleRequest, v *RespValue) {
		req.SetResponse(newBulkString("1"))
	})
	go func() {
		sconn.Read(make([]byte, 8192))
		sconn.Write([]byte("-moved 3999 127.0.0.1:6380\r\n"))
	}()
	req := newSimpleRequest(newArray([]RespValue{
		*newBulkString("get"),
		*newBulkString("a"),
	}))
	c.Send(req)
	req.Wait()
	assert.Equal(t, BulkString, req.Response().Type)
}

func TestUpstreamClientHandleResp(t *testing.T) {
	c := new(client)

	// normal response
	req := newSimpleRequest(newSimpleString("ping"))
	c.handleResp(req, respOK)
	req.Wait()
	assert.Equal(t, SimpleString, req.Response().Type)

	// error response
	req = newSimpleRequest(newArray([]RespValue{
		*newBulkString("get"),
		*newBulkString("a"),
	}))
	c.handleResp(req, newError("internal error"))
	req.Wait()
	assert.Equal(t, Error, req.Response().Type)
}

func TestUpstreamCreateClient(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	u := newUpstream(nil)
	c, err := u.createClient(l.Addr().String())
	assert.NoError(t, err)
	assert.NotNil(t, c)
	defer c.Stop()
	assert.Equal(t, 1, len(u.loadClients()))

	// same addr
	o, err := u.createClient(l.Addr().String())
	assert.NoError(t, err)
	assert.Equal(t, c, o)
}

func TestUpstreamCreateClientParallel(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	u := newUpstream(nil)
	n := runtime.GOMAXPROCS(0)
	if n < 2 {
		n = 2
	}
	cs := make([]*client, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := u.createClient(l.Addr().String())
			cs[i] = c
		}(i)
	}

	wg.Wait()
	for i := 1; i < n; i++ {
		assert.NotNil(t, cs[i])
		assert.Equal(t, cs[0], cs[i])
		cs[i].Stop()
	}
}

func TestUpstreamRemoveClient(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()

	u := newUpstream(nil)
	c, _ := u.createClient(l.Addr().String())
	defer c.Stop()
	assert.Equal(t, 1, len(u.loadClients()))
	u.removeClient(l.Addr().String())
	assert.Equal(t, 0, len(u.loadClients()))

	// remove non-existent client
	u.removeClient(l.Addr().String())
	assert.Equal(t, 0, len(u.loadClients()))
}

func TestUpstreamGetClientParallel(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()
	addr := l.Addr().String()

	u := newUpstream(nil)
	n := 8
	cs := make([]*client, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := u.createClient(addr)
			cs[i] = c
		}(i)
	}

	wg.Wait()
	for i := 1; i < n; i++ {
		assert.NotNil(t, cs[i])
		assert.Equal(t, cs[0], cs[i])
		cs[i].Stop()
	}
}

func TestUpstreamGetClientFailFast(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Error(err)
		return
	}
	defer l.Close()
	addr := l.Addr().String()

	// fill the listener backlog
	// TODO(kirk91): set the listener backlog to 1
	var conns []net.Conn
	for {
		conn, err := net.DialTimeout("tcp", l.Addr().String(), time.Millisecond*500)
		if err != nil {
			break
		}
		conns = append(conns, conn)
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()

	u := newUpstream(nil)
	n := 100 // 100 times is enough
	wg := new(sync.WaitGroup)
	cs := make([]*client, n)
	begin := time.Now()
	dialTimeout := time.Second
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, _ := u.getClient(addr)
			cs[i] = c
		}(i)
	}

	wg.Wait()
	// all client is nil
	for _, c := range cs {
		assert.Nil(t, c)
	}
	assert.Equal(t, true, time.Since(begin) < dialTimeout*2)
}

func randStr(n int) string {
	buff := make([]byte, n)
	rand.Read(buff)
	s := base64.StdEncoding.EncodeToString(buff)
	// Base 64 can be longer than len
	return s[:n]
}

func TestUpstreamRefreshSlots(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := l.Addr().String()
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		defer conn.Close()
		b := make([]byte, 1024)
		n, _ := conn.Read(b)
		// assert rawRequest
		expect := encode(newArray([]RespValue{
			*newBulkString("cluster"),
			*newBulkString("nodes"),
		}))
		if !strings.EqualFold(string(expect), string(b[:n])) {
			conn.Close()
			return
		}
		data := fmt.Sprintf("%s %s master - 0 1528688887753 7 connected 0-16383\n", randStr(40), addr)
		conn.Write(encode(newBulkString(data)))
	}()

	u := newUpstream([]string{addr})
	done := make(chan struct{})
	go func() {
		u.Serve()
		close(done)
	}()
	defer func() {
		u.Stop()
		<-done
	}()

	time.Sleep(time.Millisecond * 100)
	assert.NotEmpty(t, u.slotsLastUpdateTime)
	for i := 0; i < slotNum; i++ {
		assert.NotNil(t, u.slots[i])
	}
}

func TestUpstreamSlotsRefreshRetryOnFail(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := l.Addr().String()
	defer l.Close()

	go func() {
		conn, _ := l.Accept()
		defer conn.Close()
		// fail at first time
		conn.Read(make([]byte, 1024))
		conn.Write(encode(newError("internal error")))
		// success at second time
		conn.Read(make([]byte, 1024))
		data := fmt.Sprintf("%s %s master - 0 1528688887753 7 connected 0-16383\n", randStr(40), addr)
		conn.Write(encode(newBulkString(data)))
	}()

	u := newUpstream([]string{addr})
	slotsRefMinRate = time.Second
	done := make(chan struct{})
	go func() {
		u.Serve()
		close(done)
	}()
	defer func() {
		u.Stop()
		<-done
	}()

	time.Sleep(slotsRefMinRate + time.Second)
	assert.NotEmpty(t, u.slotsLastUpdateTime)
	for i := 0; i < slotNum; i++ {
		assert.NotNil(t, u.slots[i])
	}
}

func TestUpstreamSlotsRefreshOnExecessiveRequests(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Error(err)
		return
	}
	addr := l.Addr().String()
	defer l.Close()
	go func() {
		conn, _ := l.Accept()
		defer conn.Close()
		conn.Read(make([]byte, 1024))
		data := fmt.Sprintf("%s %s master - 0 1528688887753 7 connected 0-16383\n", randStr(40), addr)
		conn.Write(encode(newBulkString(data)))
	}()

	u := newUpstream([]string{addr})
	slotsRefMinRate = time.Second * 2
	done := make(chan struct{})
	go func() {
		u.Serve()
		close(done)
	}()
	defer func() {
		u.Stop()
		<-done
	}()

	// wait unitl slots updated
	for {
		if !u.slotsLastUpdateTime.IsZero() {
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	slotsLastUpdateTime := u.slotsLastUpdateTime

	deadline := time.Now().Add(time.Second)
	// produce execessive slots refresh requests
	for {
		if time.Now().After(deadline) {
			break
		}
		u.triggerSlotsRefresh()
	}
	// slots hasn't been updated after the last.
	assert.Equal(t, slotsLastUpdateTime, u.slotsLastUpdateTime)
}

func TestUpstreamHandleRedirection(t *testing.T) {
	u := newUpstream([]string{})
	done := make(chan struct{})
	go func() {
		u.Serve()
		close(done)
	}()
	defer func() {
		u.Stop()
		<-done
	}()

	t.Run("moved", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Error(err)
			return
		}
		defer l.Close()
		addr := l.Addr().String()

		req := newSimpleRequest(newArray([]RespValue{
			*newBulkString("get"),
			*newBulkString("a"),
		}))
		defer req.Wait()
		resp := newError("moved 123 " + addr)
		u.handleRedirection(req, resp)

		// assert rawRequest
		conn, _ := l.Accept()
		b := make([]byte, 1024)
		n, _ := conn.Read(b)
		assert.Equal(t, encode(req.Body()), b[:n])
		conn.Close()
	})

	t.Run("ask", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Error(err)
			return
		}
		defer l.Close()
		addr := l.Addr().String()

		req := newSimpleRequest(newArray([]RespValue{
			*newBulkString("get"),
			*newBulkString("a"),
		}))
		defer req.Wait()
		resp := newError("ASK 123 " + addr)
		u.handleRedirection(req, resp)

		// assert requests
		conn, _ := l.Accept()
		dec := newDecoder(conn, 2048)
		v, err := dec.Decode()
		assert.Equal(t, ASKING, string(v.Array[0].Text))
		v, _ = dec.Decode()
		assert.Equal(t, req.Body(), v)
		conn.Close()
	})
}
