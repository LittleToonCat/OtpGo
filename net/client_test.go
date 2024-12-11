package net

import (
	"bufio"
	"github.com/stretchr/testify/require"
	"net"
	. "otpgo/util"
	"testing"
	"time"
)

type MDParticipantFake struct{}

var socketBuffSize = 4096

var queue = make(chan Datagram)

func (m *MDParticipantFake) RouteDatagram(datagram Datagram) {
	queue <- datagram
}

func (m *MDParticipantFake) ReceiveDatagram(datagram Datagram) {

}

func (m *MDParticipantFake) HandleDatagram(datagram Datagram, dgi *DatagramIterator) {
	queue <- datagram
}

func (m *MDParticipantFake) Terminate(err error) {}

var participant *MDParticipantFake
var netclient *Client

var sserver net.Conn
var sclient net.Conn
var ssocket *socketTransport

func TestClient_SendDatagram(t *testing.T) {
	dg := NewDatagram()
	dg.WriteString("hello")

	go netclient.SendDatagram(dg)
	reader := bufio.NewReaderSize(sserver, socketBuffSize)
	buff := make([]byte, 9)
	_, err := reader.Read(buff)
	if err != nil {
		t.Error(err)
	}

	require.ElementsMatch(t, buff, []byte{5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'})
}

func TestClient_Read(t *testing.T) {
	dg := NewDatagram()
	dg.AddInt32(5)
	dg.WriteString("hello")

	writer := bufio.NewWriterSize(sserver, socketBuffSize)
	writer.Write(dg.Bytes())
	go writer.Flush()
	go netclient.read()
	select {
	case dg := <-queue:
		require.EqualValues(t, dg.Len(), 5)
	case <-time.After(1 * time.Second):
		t.Error("read timeout")
	}
}

func TestClient_Defragment(t *testing.T) {
	dg1 := NewDatagram()
	dg1.AddUint16(10)
	dg1.WriteString("hello ")
	dg2 := NewDatagram()
	dg2.WriteString("world")

	writer := bufio.NewWriterSize(sserver, socketBuffSize)
	writer.Write(dg1.Bytes())
	go netclient.read()
	go writer.Flush()
	writer.Write(dg2.Bytes())
	go writer.Flush()
	select {
	case dg := <-queue:
		require.EqualValues(t, dg.Len(), 10)
	case <-time.After(1 * time.Second):
		t.Error("read timeout")
	}
}

func init() {
	sserver, sclient = net.Pipe()
	ssocket = &socketTransport{
		conn:      sclient,
		rw:        sclient,
		br:        bufio.NewReaderSize(sclient, socketBuffSize),
		bw:        bufio.NewWriterSize(sclient, socketBuffSize),
		keepAlive: 60 * time.Second,
	}
	participant = &MDParticipantFake{}
	netclient = NewClient(Transport(ssocket), participant, 1*time.Second)
}
