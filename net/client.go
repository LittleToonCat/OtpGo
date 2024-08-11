package net

import (
	"bytes"
	"encoding/binary"
	"errors"
	gonet "net"
	. "otpgo/util"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pires/go-proxyproto"
)

const BUFF_SIZE = 4096

// DatagramHandler is an interface for which structures that can accept datagrams may
//  implement to accept datagrams from a client, such as an MD participant.
type DatagramHandler interface {
	// Handles a message received from the client
	ReceiveDatagram(Datagram)
	// Handles a message received from the MD
	HandleDatagram(Datagram, *DatagramIterator)

	Terminate(error)
}

type Client struct {
	sync.Mutex
	tr      Transport
	handler DatagramHandler
	buff    bytes.Buffer
	timeout time.Duration

	remote *gonet.TCPAddr
	local  *gonet.TCPAddr

	// PROXY protocol TLVs
	tlvs   []byte
	readBufferPool sync.Pool
	disconnecting atomic.Bool
}

func NewClient(tr Transport, handler DatagramHandler, timeout time.Duration) *Client {
	client := &Client{
		tr:      tr,
		handler: handler,
		remote:  tr.Conn().RemoteAddr().(*gonet.TCPAddr),
		local:   tr.Conn().LocalAddr().(*gonet.TCPAddr),
		tlvs:    []byte{},
		readBufferPool: sync.Pool{
			New: func() any {
				buff := make([]byte, BUFF_SIZE)
				return &buff
			},
		},
	}
	client.initialize()
	client.timeout = timeout
	return client
}

func (c *Client) initialize() {
	// FIXME: Properly test this.
	if proxyConn, ok := c.tr.Conn().(*proxyproto.Conn); ok {
		header := proxyConn.ProxyHeader()
		if header != nil {
			tlvs, err := header.TLVs()
			if err != nil {
				return
			}
			c.tlvs, err = proxyproto.JoinTLVs(tlvs)
			if err != nil {
				return
			}
		}
	}
	go c.read()
}

func (c *Client) shutdown() {
	c.tr.Close()
}

func (c *Client) defragment() {
	for c.buff.Len() > Blobsize {
		data := c.buff.Bytes()
		sz := binary.LittleEndian.Uint16(data[0:Blobsize])
		if c.buff.Len() >= int(sz+Blobsize) {
			overreadSz := c.buff.Len() - int(sz) - int(Blobsize)
			dg := NewDatagram()
			dg.Write(data[Blobsize : sz+Blobsize])
			if 0 < overreadSz {
				c.buff.Truncate(0)
				c.buff.Write(data[sz+Blobsize : sz+Blobsize+uint16(overreadSz)])
			} else {
				// No overread
				c.buff.Truncate(0)
			}

			c.handler.ReceiveDatagram(dg)
		} else {
			return
		}
	}
}

func (c *Client) processInput(len int, data []byte) {
	if !c.ConnectedAndIsNotDisconnecting() {
		return
	}
	c.Lock()

	// Check if we have enough data for a single datagram
	if c.buff.Len() == 0 && len >= Blobsize {
		sz := binary.LittleEndian.Uint16(data[0:Blobsize])
		if sz == uint16(len-Blobsize) {
			// We have enough data for a full datagram; send it off
			dg := NewDatagram()
			dg.Write(data[Blobsize:])
			c.handler.ReceiveDatagram(dg)
			c.Unlock()
			return
		}
	}

	c.buff.Write(data)
	c.defragment()
	c.Unlock()

}

func (c *Client) read() {
	for {
		if !c.ConnectedAndIsNotDisconnecting() {
			return
		}

		buff := c.readBufferPool.Get().(*[]byte)
		// Make sure the buffer is zeroed.
		clear(*buff)
		if n, err := c.tr.Read(*buff); err == nil {
			c.processInput(n, (*buff)[0:n])
			c.readBufferPool.Put(buff)
		} else {
			c.disconnect(err, true)
			return
		}
	}
}

func (c *Client) SendDatagram(datagram Datagram) {
	if !c.ConnectedAndIsNotDisconnecting() {
		return
	}

	dg := NewDatagram()

	c.Lock()
	defer c.Unlock()

	dg.AddUint16(uint16(datagram.Len()))
	dg.Write(datagram.Bytes())

	if _, err := c.tr.WriteDatagram(dg); err != nil {
		c.disconnect(err, false)
		return
	}

	writeTimer := time.NewTimer(c.timeout)

	select {
	case err := <-c.tr.Flush():
		if !writeTimer.Stop() {
			<-writeTimer.C
		}
		if err != nil {
			c.disconnect(err, false)
			return
		}
	case <-writeTimer.C:
		c.disconnect(errors.New("write timeout"), false)
		return
	}
}

// Close closes the client's transport if it isn't already closed. 
// needsLock indicates whether this function should try and acquire the client mutex; if the caller already has the mutex, set this to false.
func (c *Client) Close(needsLock bool) {
	if !c.disconnecting.CompareAndSwap(false, true) || !c.Connected() {
		return
	}

	if needsLock {
		c.Lock()
		defer c.Unlock()
	}

	c.tr.Close()
}

// disconnect disconnects the client. 
// needsLock indicates whether the transport closing should try and acquire the client mutex; if the caller already has the mutex, set this to false. 
func (c *Client) disconnect(err error, needsLock bool) {
	c.Close(needsLock)
	c.handler.Terminate(err)
}

func (c *Client) Local() bool {
	return true
}

func (c *Client) Connected() bool {
	return !c.tr.Closed()
}

func (c *Client) ConnectedAndIsNotDisconnecting() bool {
	return c.Connected() && !c.disconnecting.Load()
}

func (c *Client) RemoteIP() string {
	return c.remote.IP.String()
}

func (c *Client) RemotePort() uint16 {
	return uint16(c.remote.Port)
}

func (c *Client) LocalIP() string {
	return c.local.IP.String()
}

func (c *Client) LocalPort() uint16 {
	return uint16(c.local.Port)
}

func (c *Client) Tlvs() []byte {
	return c.tlvs
}
