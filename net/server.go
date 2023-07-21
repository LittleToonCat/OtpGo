// Original code derived from https://github.com/ortuman/jackal

package net

import (
	"otpgo/core"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// Server is an interface which allows a network listening mechanism to pass accepted connections to
//  an actual server, like a CA or MD
type Server interface {
	HandleConnect(net.Conn)
}

// NetworkServer is a base class which provides methods that accept connections.
type NetworkServer struct {
	Handler Server

	keepAlive time.Duration
	ln        net.Listener
	listening uint32
}

func (s *NetworkServer) Start(bindAddr string, errChan chan error) {
	if err := s.listenConn(bindAddr, errChan); err != nil {
		errChan <- err
	}
}

func (s *NetworkServer) Shutdown() error {
	if atomic.CompareAndSwapUint32(&s.listening, 1, 0) {
		if err := s.ln.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *NetworkServer) listenConn(address string, errChan chan error) error {
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s.ln = ln

	errChan <- nil
	s.handleInterrupts()
	atomic.StoreUint32(&s.listening, 1)
	for atomic.LoadUint32(&s.listening) == 1 {
		conn, err := ln.Accept()
		if err == nil {
			s.Handler.HandleConnect(conn)
			continue
		}
	}
	return nil
}

func (s *NetworkServer) handleInterrupts() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-c:
			s.Shutdown()
		case <-core.StopChan:
			s.Shutdown()
		}
	}()
}

func (s *NetworkServer) Listening() uint32 {
	return atomic.LoadUint32(&s.listening)
}
