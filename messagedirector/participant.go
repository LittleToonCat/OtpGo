package messagedirector

import (
	"otpgo/net"
	. "otpgo/util"
	"errors"
	gonet "net"
	"sync"
	"time"
)

type MDParticipant interface {
	net.DatagramHandler

	// RouteDatagram routes a datagram through the MD
	RouteDatagram(Datagram)

	SubscribeChannel(Channel_t)
	UnsubscribeChannel(Channel_t)

	SubscribeRange(Range)
	UnsubscribeRange(Range)

	SetName(string)
	Name() string

	Subscriber() *Subscriber
}

type MDParticipantBase struct {
	MDParticipant

	subscriber  *Subscriber
	postRemoves []Datagram

	name       string
	url        string
	terminated bool

	mu sync.Mutex
}

func (m *MDParticipantBase) Init(handler MDParticipant) {
	m.postRemoves = []Datagram{}
	m.subscriber = &Subscriber{participant: handler, active: true}
	MD.participants = append(MD.participants, m)
}

func (m *MDParticipantBase) Subscriber() *Subscriber {
	return m.subscriber
}

func (m *MDParticipantBase) RouteDatagram(datagram Datagram) {
	MD.queueLock.Lock()
	MD.Queue = append(MD.Queue, QueueEntry{datagram, m})
	MD.queueLock.Unlock()

	select {
	case MD.shouldProcess <- true:
	default:
	}
}

func (m *MDParticipantBase) PostRemove() {
	for _, dg := range m.postRemoves {
		m.RouteDatagram(dg)
	}

	MD.RecallPostRemoves()
}

func (m *MDParticipantBase) AddPostRemove(dg Datagram) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.postRemoves = append(m.postRemoves, dg)
	MD.PreroutePostRemove(dg)
}

func (m *MDParticipantBase) ClearPostRemoves() {
	m.mu.Lock()
	defer m.mu.Unlock()

	clear(m.postRemoves)
	MD.RecallPostRemoves()
}

func (m *MDParticipantBase) SubscribeChannel(ch Channel_t) {
	channelMap.SubscribeChannel(m.subscriber, ch)
}

func (m *MDParticipantBase) UnsubscribeChannel(ch Channel_t) {
	channelMap.UnsubscribeChannel(m.subscriber, ch)
}

func (m *MDParticipantBase) SubscribeRange(rng Range) {
	channelMap.SubscribeRange(m.subscriber, rng)
}

func (m *MDParticipantBase) UnsubscribeRange(rng Range) {
	channelMap.UnsubscribeRange(m.subscriber, rng)
}

func (m *MDParticipantBase) SetName(name string) {
	m.name = name
}

func (m *MDParticipantBase) Name() string {
	return m.name
}

func (m *MDParticipantBase) IsTerminated() bool {
	return m.terminated
}

func (m *MDParticipantBase) Cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.terminated = true
	m.PostRemove()
	channelMap.UnsubscribeAll(m.subscriber)
	MD.RemoveParticipant(m)
}

func (m *MDParticipantBase) RecycleParticipant() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscriber = nil
	clear(m.postRemoves)
	m.name = ""
	m.url = ""
	m.terminated = false
}

func (m *MDParticipantBase) Terminate(err error) { /* virtual */ }

// MDNetworkParticipant represents a downstream MD connection
type MDNetworkParticipant struct {
	MDParticipantBase

	client *net.Client
	conn   gonet.Conn
	mu     sync.Mutex
}

func NewMDParticipant(conn gonet.Conn) *MDNetworkParticipant {
	participant := &MDNetworkParticipant{conn: conn}
	participant.MDParticipantBase.Init(participant)
	socket := net.NewSocketTransport(conn, 0, 4096)

	participant.client = net.NewClient(socket, participant, 60*time.Second)
	participant.SetName(conn.RemoteAddr().String())
	return participant
}

func (m *MDNetworkParticipant) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	m.client.SendDatagram(dg)
}

func (m *MDNetworkParticipant) ReceiveDatagram(dg Datagram) {
	m.mu.Lock()
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				m.Terminate(errors.New("MDNetworkParticipant received a truncated datagram"))
			} else {
				m.Terminate(r.(error))
			}
		}
	}()

	dgi := NewDatagramIterator(&dg)
	channels := dgi.ReadUint8()
	if channels == 1 && dgi.ReadChannel() == CONTROL_MESSAGE {
		msg := dgi.ReadUint16()
		switch msg {
		case CONTROL_SET_CHANNEL:
			m.SubscribeChannel(dgi.ReadChannel())
		case CONTROL_REMOVE_CHANNEL:
			m.UnsubscribeChannel(dgi.ReadChannel())
		case CONTROL_ADD_RANGE:
			m.SubscribeRange(Range{dgi.ReadChannel(), dgi.ReadChannel()})
		case CONTROL_REMOVE_RANGE:
			m.UnsubscribeRange(Range{dgi.ReadChannel(), dgi.ReadChannel()})
		case CONTROL_ADD_POST_REMOVE:
			m.AddPostRemove(*dgi.ReadDatagram())
		case CONTROL_CLEAR_POST_REMOVES:
			m.ClearPostRemoves()
		case CONTROL_SET_CON_NAME:
			m.name = dgi.ReadString()
		case CONTROL_SET_CON_URL:
			m.url = dgi.ReadString()
		case CONTROL_LOG_MESSAGE:
			// Our version of the eventlogger cannot log datagrams whatsoever, so this
			//  feature is essentially deprecated.
		default:
			MDLog.Errorf("MDNetworkParticipant got unknown control message with message type: %d", msg)
		}
		m.mu.Unlock()
		return
	}

	m.RouteDatagram(dg)
	m.mu.Unlock()
}

func (m *MDNetworkParticipant) Terminate(err error) {
	if m.terminated {
		return
	}
	MDLog.Infof("Lost connection from %s: %s", m.conn.RemoteAddr(), err.Error())
	m.Cleanup()
	m.client.Close()
}
