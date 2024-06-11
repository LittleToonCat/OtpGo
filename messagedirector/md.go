package messagedirector

import (
	"fmt"
	gonet "net"
	"os"
	"os/signal"
	"otpgo/core"
	"otpgo/net"
	. "otpgo/util"
	"sync"

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
	participants []MDParticipant

	// MD participants may directly queue datagarams to be routed by appending it into the
	// queue slice, where they will be processed asynchronously
	Queue []QueueEntry
	queueLock sync.Mutex

	// RouteDatagram will insert to this channel to let the queue loop know there are
	// datagrams to be processed.
	shouldProcess chan bool

	// If an MD is configurated to be upstream, it will connect to the downstream MD and route channelmap
	// events through it. Clients subscribing to channels that reside in other parts of the network will
	// receive updates for them through the downstream MD.
	upstream *MDUpstream
}

func init() {
	MDLog = log.WithFields(log.Fields{
		"name": "MD",
		"modName": "MD",
	})
}

func Start() {
	MD = &MessageDirector{}
	MD.Queue = make([]QueueEntry, 0)
	MD.shouldProcess = make(chan bool)
	MD.participants = make([]MDParticipant, 0)
	MD.Handler = MD

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

func (m *MessageDirector) getDatagramFromQueue() QueueEntry {
	m.queueLock.Lock()
	defer m.queueLock.Unlock()

	obj := MD.Queue[0]
	MD.Queue[0] = QueueEntry{}
	MD.Queue = MD.Queue[1:]
	if len(MD.Queue) == 0 {
		// Recreate the queue slice. This prevents the capacity from growing indefinitely and allows old entries to drop off as soon as possible from the backing array.
		MD.Queue = make([]QueueEntry, 0)
	}
	return obj
}

func (m *MessageDirector) queueLoop() {
	finish := make(chan bool)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	for {
		select {
		case <-MD.shouldProcess:
			for len(MD.Queue) > 0 {
				obj := m.getDatagramFromQueue()
				go func() {
					// We are running in a goroutine so that our main read loop will not crash if a datagram EOF is thrown.
					defer func() {
						if r := recover(); r != nil {
							if _, ok := r.(DatagramIteratorEOF); ok {
								MDLog.Error("Reached end of datagram")
								// TODO
							}
							finish <- true
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
					mdDg := &MDDatagram{dg: seekDgi, sender: obj.md}
					for _, recv := range receivers {
						channelMap.Send(recv, mdDg)
					}

					// Send message upstream if necessary
					if obj.md != nil && m.upstream != nil {
						m.upstream.HandleDatagram(obj.dg, nil)
					}
					finish <- true
				}()
				<-finish
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
	tempParticipantSlice := make([]MDParticipant, 0)
	for _, participant := range MD.participants {
		if participant != p {
			tempParticipantSlice = append(tempParticipantSlice, participant)
		}
	}
	MD.participants = tempParticipantSlice
	m.Unlock()
}
