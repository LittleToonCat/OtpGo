// Original code derived from https://github.com/ortuman/jackal

package net

import (
	"io"
	"net"
	"otpgo/util"
)

// Transport represents a stream transport mechanism.
type Transport interface {
	io.ReadWriteCloser

	// WriteString writes a datagram to the transport
	WriteDatagram(datagram util.Datagram) (n int, err error)

	// Flush writes any buffered data to the underlying io.Writer.
	Flush() chan error

	// Closed returns if the transport is closed or not
	Closed() bool

	// Conn returns the transports underlying connection
	Conn() net.Conn
}
