// Original code derived from https://github.com/ortuman/jackal

package net

import (
	"bufio"
	"io"
	"net"
	"otpgo/util"
	"sync/atomic"
	"time"
)

type socketTransport struct {
	conn      net.Conn
	rw        io.ReadWriter
	br        *bufio.Reader
	bw        *bufio.Writer
	keepAlive time.Duration
	closed    atomic.Bool
}

// NewSocketTransport creates a socket class stream transport.
func NewSocketTransport(conn net.Conn, keepAlive time.Duration, buffSize int) Transport {
	s := &socketTransport{
		conn:      conn,
		rw:        conn,
		br:        bufio.NewReaderSize(conn, buffSize),
		bw:        bufio.NewWriterSize(conn, buffSize),
		keepAlive: keepAlive,
	}
	return s
}

func (s *socketTransport) Read(p []byte) (n int, err error) {
	if s.keepAlive > 0 {
		s.conn.SetReadDeadline(time.Now().Add(s.keepAlive))
	}

	return s.br.Read(p)
}

func (s *socketTransport) Write(p []byte) (n int, err error) {
	return s.bw.Write(p)
}

func (s *socketTransport) WriteDatagram(datagram util.Datagram) (n int, err error) {
	return s.bw.Write(datagram.Bytes())
}

func (s *socketTransport) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		// Don't try and close again if something already closed the socket.
		return nil
	}

	return s.conn.Close()
}

func (s *socketTransport) Closed() bool {
	return s.closed.Load()
}

func (s *socketTransport) Conn() net.Conn {
	return s.conn
}

// Flush writes any buffered data to the underlying io.Writer.
func (s *socketTransport) Flush() chan error {
	errChan := make(chan error)
	go func() {
		errChan <- s.bw.Flush()
	}()
	return errChan
}
