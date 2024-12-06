package net

import (
	"bufio"
	"github.com/stretchr/testify/require"
	"net"
	"otpgo/util"
	"testing"
	"time"
)

var server net.Conn
var client net.Conn
var socket *socketTransport

func TestSocketTransport_Write(t *testing.T) {
	dg := util.NewDatagram()
	dg.AddInt64(5)
	if _, err := socket.WriteDatagram(dg); err != nil {
		t.Error(err)
	}

	go func() {
		err := <-socket.Flush()
		if err != nil {
			t.Error(err)
		}
	}()

	reader := bufio.NewReaderSize(server, socketBuffSize)
	byte, err := reader.ReadByte()
	if err != nil {
		t.Error(err)
	}
	require.EqualValues(t, byte, 5)
}

func TestSocketTransport_Read(t *testing.T) {
	dg := util.NewDatagram()
	dg.AddInt64(5)

	writer := bufio.NewWriterSize(server, socketBuffSize)
	if _, err := writer.Write(dg.Bytes()); err != nil {
		t.Error(err)
	}

	go writer.Flush()
	buff := make([]byte, 1024)
	_, err := socket.Read(buff)
	if err != nil {
		t.Error(nil)
	}

	require.EqualValues(t, buff[0], 5)
}

func TestSocketTransport_TestReadDeadline(t *testing.T) {
	buff := make([]byte, 1024)
	_, err := socket.Read(buff)
	require.NotNil(t, err)
}

func TestSocketTransport_TestEOF(t *testing.T) {
	server.Close()
	buff := make([]byte, 1024)
	_, err := socket.Read(buff)
	require.NotNil(t, err)
}

func init() {
	server, client = net.Pipe()
	socket = &socketTransport{
		conn:      client,
		rw:        client,
		br:        bufio.NewReaderSize(client, socketBuffSize),
		bw:        bufio.NewWriterSize(client, socketBuffSize),
		keepAlive: 50 * time.Millisecond,
	}
}
