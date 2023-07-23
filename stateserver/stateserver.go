package stateserver

import (
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"
	"fmt"
	"github.com/apex/log"
)

type StateServer struct {
	messagedirector.MDParticipantBase

	config  core.Role
	log     *log.Entry
	objects map[Doid_t]*DistributedObject
}

func NewStateServer(config core.Role) *StateServer {
	ss := &StateServer{
		config:  config,
		objects: make(map[Doid_t]*DistributedObject),
		log: log.WithFields(log.Fields{
			"name": fmt.Sprintf("StateServer (%d)", config.Control),
		}),
	}

	ss.Init(ss)

	if Channel_t(config.Control) != INVALID_CHANNEL {
		ss.SubscribeChannel(Channel_t(config.Control))
		ss.SubscribeChannel(BCHAN_STATESERVERS)
	}

	return ss
}

func (s *StateServer) handleGenerate(dgi *DatagramIterator, other bool) {
	do := dgi.ReadDoid()
	parent := dgi.ReadDoid()
	zone := dgi.ReadZone()
	dc := dgi.ReadUint16()

	if _, ok := s.objects[do]; ok {
		s.log.Warnf("Received generate for already-existing object ID=%d", do)
		return
	}

	if core.DC.Get_num_classes() < int(dc) {
		s.log.Errorf("Received create for unknown dclass id %d", dc)
		return
	}

	dclass := core.DC.Get_class(int(dc))
	if ok, obj, err := NewDistributedObject(s, do, parent, zone, dclass, dgi, other); ok {
		s.objects[do] = obj
	} else {
		s.log.Errorf("Unable to create object; %s!", err.Error())
	}
}

func (s *StateServer) handleDelete(dgi *DatagramIterator, sender Channel_t) {
	var targets []Channel_t
	ai := dgi.ReadChannel()

	for do, obj := range s.objects {
		if obj.aiChannel == ai && obj.explicitAi {
			targets = append(targets, Channel_t(do))
		}
	}

	if len(targets) > 0 {
		dg := NewDatagram()
		dg.AddMultipleServerHeader(targets, sender, STATESERVER_DELETE_AI_OBJECTS)
		dg.AddChannel(ai)
		s.RouteDatagram(dg)
	}
}

func (s *StateServer) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				s.log.Errorf("Received truncated datagram")
			}
		}
	}()

	sender := dgi.ReadChannel()
	msgType := dgi.ReadUint16()

	switch msgType {
	case STATESERVER_CREATE_OBJECT_WITH_REQUIRED:
		s.handleGenerate(dgi, false)
	case STATESERVER_CREATE_OBJECT_WITH_REQUIRED_OTHER:
		s.handleGenerate(dgi, true)
	case STATESERVER_DELETE_AI_OBJECTS:
		s.handleDelete(dgi, sender)
	default:
		s.log.Warnf("Received unknown msgtype=%d", msgType)
	}
}
