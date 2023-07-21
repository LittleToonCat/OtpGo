package messagedirector

import (
	"otpgo/net"
	. "otpgo/util"
	gonet "net"
	"os"
	"time"
)

type MDUpstream struct {
	MDParticipantBase

	md     *MessageDirector
	client *net.Client
}

func NewMDUpstream(md *MessageDirector, address string) *MDUpstream {
	up := &MDUpstream{md: md}

	conn, err := gonet.Dial("tcp", address)
	if err != nil {
		MDLog.Fatalf("upstream failed to connect: %s", err)
		return nil
	}
	socket := net.NewSocketTransport(conn, 0, 4096)
	up.client = net.NewClient(socket, up, 60*time.Second)
	return up
}

func (m *MDUpstream) SubscribeChannel(ch Channel_t) {
	dg := NewDatagram()
	dg.AddControlHeader(CONTROL_ADD_CHANNEL)
	dg.AddChannel(ch)
	m.client.SendDatagram(dg)
}

func (m *MDUpstream) UnsubscribeChannel(ch Channel_t) {
	dg := NewDatagram()
	dg.AddControlHeader(CONTROL_REMOVE_CHANNEL)
	dg.AddChannel(ch)
	m.client.SendDatagram(dg)
}

func (m *MDUpstream) SubscribeRange(lo Channel_t, hi Channel_t) {
	dg := NewDatagram()
	dg.AddControlHeader(CONTROL_ADD_RANGE)
	dg.AddChannel(lo)
	dg.AddChannel(hi)
	m.client.SendDatagram(dg)
}

func (m *MDUpstream) UnsubscribeRange(lo Channel_t, hi Channel_t) {
	dg := NewDatagram()
	dg.AddControlHeader(CONTROL_REMOVE_RANGE)
	dg.AddChannel(lo)
	dg.AddChannel(hi)
	m.client.SendDatagram(dg)
}

func (m *MDUpstream) HandleDatagram(datagram Datagram, dgi *DatagramIterator) {
	m.client.SendDatagram(datagram)
}

func (m *MDUpstream) ReceiveDatagram(datagram Datagram) {
	MD.Queue <- struct {
		dg Datagram
		md MDParticipant
	}{datagram, nil}
}

func (m *MDUpstream) Terminate(err error) {
	MDLog.Fatalf("Lost connection to upstream MD: %s", err)
	m.client.Close()
	os.Exit(0)
}
