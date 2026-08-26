package messagedirector

import (
	"fmt"
	gonet "net"
	"os"
	"os/signal"
	"otpgo/core"
	"otpgo/net"
	. "otpgo/util"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/apex/log"
)

type QueueEntry struct {
	dg Datagram
	md MDParticipant
}

var MDLog *log.Entry
var MD *MessageDirector

type MessageDirector struct {
	sync.Mutex
	net.Server
	net.NetworkServer

	// Connections within the context of the MessageDirector are represented as
	// participants; however, clients and objects on the SS may function as participants
	// as well. The MD will keep track of them and what channels they subscribe and route data to them.
	// The IDs should not be assumed to be sequential, nor that a key will always hold the same participant.
	participants *MutexMap[uint32, MDParticipant]
	// freeParticipantIds contains IDs that were once allocated, but are now free to use. They take priority over assigning a new ID.
	freeParticipantIds *MutexMap[uint32, bool]
	// previousAllocatedParticipantId was the last ID assigned when a participant needed a fresh ID.
	previousAllocatedParticipantId atomic.Uint32

	// MD participants may directly queue datagrams to be routed by adding it into the
	// queue map, where they will be processed asynchronously
	Queue     [][]QueueEntry
	queueLock sync.Mutex

	// RouteDatagram will insert to this channel to let the queue loop know there are
	// datagrams to be processed.
	shouldProcess chan bool

	forwards map[uint16]Channel_t

	// If an MD is configurated to be upstream, it will connect to the downstream MD and route channelmap
	// events through it. Clients subscribing to channels that reside in other parts of the network will
	// receive updates for them through the downstream MD.
	upstream *MDUpstream
}

func init() {
	MDLog = log.WithFields(log.Fields{
		"name":    "MD",
		"modName": "MD",
	})
}

func Start() {
	MD = &MessageDirector{}
	MD.shouldProcess = make(chan bool)
	MD.participants = NewMutexMap[uint32, MDParticipant]()
	MD.freeParticipantIds = NewMutexMap[uint32, bool]()
	MD.previousAllocatedParticipantId.Store(0)
	MD.Handler = MD

	MD.forwards = make(map[uint16]Channel_t)
	for _, forward := range core.Config.MessageDirector.Forwarding {
		MD.forwards[forward.Msgtype] = forward.Channel
	}

	channelMap := ChannelMap{}
	channelMap.init()

	bindAddr := core.Config.MessageDirector.Bind
	if bindAddr != "" {
		errChan := make(chan error)
		go func() {
			err := <-errChan
			switch err {
			case nil:
				MDLog.Info(fmt.Sprintf("Opened listening socket at %s", bindAddr))
			default:
				MDLog.Fatal(err.Error())
			}
		}()
		go MD.Start(bindAddr, errChan, false)
	}

	go MD.queueLoop()

	connectAddr := core.Config.MessageDirector.Connect
	if connectAddr != "" {
		MD.upstream = NewMDUpstream(MD, connectAddr)
	}
}

func (m *MessageDirector) queueIsEmpty() bool {
	m.queueLock.Lock()
	defer m.queueLock.Unlock()

	return len(MD.Queue) == 0 || (len(MD.Queue) == 1 && len(MD.Queue[0]) == 0)
}

func (m *MessageDirector) getDatagramFromQueue() QueueEntry {
	m.queueLock.Lock()
	defer m.queueLock.Unlock()

	for len(MD.Queue) > 0 && len(MD.Queue[0]) == 0 {
		MD.Queue = MD.Queue[1:]
	}

	obj := MD.Queue[0][0]
	MD.Queue[0] = MD.Queue[0][1:]
	return obj
}

func (m *MessageDirector) enqueue(dg Datagram, p MDParticipant) {
	m.queueLock.Lock()
	m.Queue = append(m.Queue, []QueueEntry{{dg, p}})
	m.queueLock.Unlock()

	select {
	case m.shouldProcess <- true:
	default:
	}
}

func (m *MessageDirector) enqueueEarly(dg Datagram, p MDParticipant) {
	m.queueLock.Lock()
	if len(m.Queue) == 0 {
		m.Queue = append(m.Queue, []QueueEntry{{dg, p}})
	} else {
		m.Queue[0] = append(m.Queue[0], QueueEntry{dg, p})
	}
	m.queueLock.Unlock()

	select {
	case m.shouldProcess <- true:
	default:
	}
}

func (m *MessageDirector) dispatchEntry(obj QueueEntry) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				MDLog.Error("Reached end of datagram")
				// TODO
			}
		}
	}()

	// Iterate the datagram for receivers
	var receivers []Channel_t
	dgi := NewDatagramIterator(&obj.dg)
	chanCount := dgi.ReadUint8()
	for n := 0; uint8(n) < chanCount; n++ {
		receivers = append(receivers, dgi.ReadChannel())
	}

	// MDLog.Debugf("Routing datagram to channels: %v", receivers)

	// Send payload datagram to every available receiver
	seekDgi := NewDatagramIterator(&obj.dg)
	seekDgi.Seek(dgi.Tell())
	mdDg := NewMDDatagram(seekDgi, obj.md)
	for _, recv := range receivers {
		channelMap.Send(recv, mdDg)
	}

	if len(m.forwards) > 0 {
		peek := NewDatagramIterator(&obj.dg)
		peek.Seek(dgi.Tell())
		if peek.RemainingSize() >= Chansize+2 {
			peek.ReadChannel()
			if forward, ok := m.forwards[peek.ReadUint16()]; ok && !slices.Contains(receivers, forward) {
				channelMap.Send(forward, mdDg)
			}
		}
	}

	// Send message upstream if necessary
	if obj.md != nil && m.upstream != nil {
		m.upstream.HandleDatagram(obj.dg, nil)
	}
}

func (m *MessageDirector) queueLoop() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	for {
		select {
		case <-MD.shouldProcess:
			for !m.queueIsEmpty() {
				obj := m.getDatagramFromQueue()
				m.dispatchEntry(obj)
			}
		case <-signalCh:
			return
		case <-core.StopChan:
			return
		}
	}
}

// AddChannel and similar functions subscribe an upstream MD to events that may occur downstream regarding
// objects that exist in the upstream's channel map.
func (m *MessageDirector) AddChannel(ch Channel_t) {
	if m.upstream != nil {
		m.upstream.SubscribeChannel(ch)
	}
}

func (m *MessageDirector) RemoveChannel(ch Channel_t) {
	if m.upstream != nil {
		m.upstream.UnsubscribeChannel(ch)
	}
}

func (m *MessageDirector) AddRange(lo Channel_t, hi Channel_t) {
	if m.upstream != nil {
		m.upstream.SubscribeRange(lo, hi)
	}
}

func (m *MessageDirector) RemoveRange(lo Channel_t, hi Channel_t) {
	if m.upstream != nil {
		m.upstream.UnsubscribeRange(lo, hi)
	}
}

func (m *MessageDirector) HandleConnect(conn gonet.Conn) {
	MDLog.Infof("Incoming connection from %s", conn.RemoteAddr())
	NewMDParticipant(conn)
}

func (m *MessageDirector) PreroutePostRemove(pr Datagram) {
	if m.upstream != nil {
		dg := NewDatagram()
		dg.AddControlHeader(CONTROL_ADD_POST_REMOVE)
		dg.AddBlob(&pr)
		m.upstream.HandleDatagram(dg, nil)
	}
}

func (m *MessageDirector) RecallPostRemoves() {
	if m.upstream != nil {
		dg := NewDatagram()
		dg.AddControlHeader(CONTROL_CLEAR_POST_REMOVES)
		m.upstream.HandleDatagram(dg, nil)
	}
}

func (m *MessageDirector) RemoveParticipant(p MDParticipant) {
	m.Lock()
	id := p.Id()
	m.participants.Delete(id, false)
	// Assign this ID for use later.
	m.freeParticipantIds.Set(id, true, false)
	m.Unlock()
}
