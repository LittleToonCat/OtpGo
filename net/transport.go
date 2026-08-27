// Original code derived from https://github.com/ortuman/jackal

package net

import (
	"io"
	"net"
	"time"
)

// Transport represents a stream transport mechanism.
type Transport interface {
	io.ReadWriteCloser

	SetWriteDeadline(t time.Time) error

	// Flush writes any buffered data to the underlying connection.
	Flush() error

	// Closed returns if the transport is closed or not
	Closed() bool

	// Conn returns the transports underlying connection
	Conn() net.Conn
}
