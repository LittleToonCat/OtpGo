package stateserver

import (
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"
	"fmt"
	"github.com/apex/log"
	dc "github.com/LittleToonCat/dcparser-go"
)

type StateServer struct {
	messagedirector.MDParticipantBase

	config  core.Role
	log     *log.Entry
	control Channel_t
	objects map[Doid_t]*DistributedObject
	mainObj *DistributedObject;
}

func NewStateServer(config core.Role) *StateServer {
	ss := &StateServer{}
	ss.InitStateServer(config, fmt.Sprintf("StateServer (%d)", config.Control), "StateServer", fmt.Sprintf("%d", config.Control))

	ss.Init(ss)

	if Channel_t(config.Control) != INVALID_CHANNEL {
		ss.control = Channel_t(config.Control)
		ss.SubscribeChannel(Channel_t(ss.control))
		ss.SubscribeChannel(BCHAN_STATESERVERS)
	}

	ss.registerObjects(config.Objects)

	return ss
}

func (s *StateServer) InitStateServer(config core.Role, logName string, logModName string, logId string,) {
	s.config = config
	s.objects = map[Doid_t]*DistributedObject{}
	s.log = log.WithFields(log.Fields{
		"name": logName,
		"modName": logModName,
		"id": logId,
	})
	s.SetName(logName)
}

func (s *StateServer) registerObjects(objects []struct{ID int; Class string}) {
	// Create an ObjectServer or DistributedObject object to rep ourself.
	dclass := core.DC.Get_class_by_name("ObjectServer")

	if (dclass == dc.SwigcptrDCClass(0)) {
		// Older client support
		dclass = core.DC.Get_class_by_name("DistributedObject")
	}

	if (dclass != dc.SwigcptrDCClass(0)) {
		dg := NewDatagram()
		dg.AddString(dclass.Get_name()) // setName
		dg.AddUint32(uint32(core.DC.Get_hash())) // setDcHash

		dgi := NewDatagramIterator(&dg)
		if ok, obj, err := NewDistributedObject(s, Doid_t(s.control), 0, 0, dclass, dgi, false, true); ok {
			s.mainObj = obj
		} else {
			s.log.Errorf("Unable to create object; %s!", err.Error())
		}
	}

	for _, obj := range objects {
		dclass := core.DC.Get_class_by_name(obj.Class)
		// Check if the method returns a NULL pointer
		if dclass == dc.SwigcptrDCClass(0) {
			s.log.Fatalf("For Configured class %d, class %s does not exist!", obj.ID, obj.Class)
			return
		}

		// TODO: Configurable field values.  We have functions that parses
		// human formatted data into binary.
		requiredFields := FieldValues{}
		ramFields := FieldValues{}

		do := NewDistributedObjectWithData(s, Doid_t(obj.ID), 0, 0, dclass, requiredFields, ramFields)
		s.objects[Doid_t(obj.ID)] = do
	}
}

func (s *StateServer) CreateDistributedObjectWithData( doid Doid_t, parent Doid_t,
	zone Zone_t, dclass dc.DCClass, requiredFields FieldValues,
	ramFields FieldValues) *DistributedObject {
		do := NewDistributedObjectWithData(s, doid, parent, zone, dclass, requiredFields, ramFields)
		s.objects[doid] = do

		return do
	}

func (s *StateServer) handleGenerate(dgi *DatagramIterator, other bool) {
	parent := dgi.ReadDoid()
	zone := dgi.ReadZone()
	dc := dgi.ReadUint16()
	do := dgi.ReadDoid()

	if _, ok := s.objects[do]; ok {
		s.log.Warnf("Received generate for already-existing object ID=%d", do)
		return
	}

	if core.DC.Get_num_classes() < int(dc) {
		s.log.Errorf("Received create for unknown dclass id %d", dc)
		return
	}

	dclass := core.DC.Get_class(int(dc))
	if ok, obj, err := NewDistributedObject(s, do, parent, zone, dclass, dgi, other, false); ok {
		s.objects[do] = obj
	} else {
		s.log.Errorf("Unable to create object; %s!", err.Error())
	}
}

func (s *StateServer) handleDelete(dgi *DatagramIterator, sender Channel_t) {
	do := dgi.ReadDoid()

	if obj, ok := s.objects[do]; ok {
		obj.annihilate(sender, true)
		return
	}

	// Reply as not found
	dg := NewDatagram()
	dg.AddServerHeader(sender, s.control, STATESERVER_OBJECT_NOTFOUND)
	dg.AddDoid(do)
	s.RouteDatagram(dg)
}

func (s *StateServer) handleDeleteAi(dgi *DatagramIterator, sender Channel_t) {
	var targets []Channel_t
	ai := dgi.ReadChannel()

	for do, obj := range s.objects {
		if obj.aiChannel == ai && obj.explicitAi {
			targets = append(targets, Channel_t(do))
		}
	}

	if len(targets) > 0 {
		dg := NewDatagram()
		dg.AddMultipleServerHeader(targets, sender, STATESERVER_SHARD_REST)
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
	case STATESERVER_OBJECT_GENERATE_WITH_REQUIRED:
		fallthrough
	case STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
		s.handleGenerate(dgi, msgType == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	case STATESERVER_SHARD_REST:
		s.handleDeleteAi(dgi, sender)
	case STATESERVER_QUERY_OBJECT_ALL:
		context := dgi.ReadUint32()

		if s.mainObj == nil {
			s.log.Warn("Got GetAI but no main object.  Did you mean to send it to an object?")
			return
		}

		dg = NewDatagram()
		dg.AddServerHeader(sender, Channel_t(s.control), STATESERVER_QUERY_OBJECT_ALL_RESP)
		dg.AddUint32(context)
		s.mainObj.appendRequiredDataDoidLast(dg, false, false)
		s.mainObj.appendOtherData(dg, false, false)
		s.RouteDatagram(dg)
	case STATESERVER_OBJECT_DELETE_RAM:
		s.handleDelete(dgi, sender)
	default:
		s.log.Warnf("Received unknown msgtype=%d", msgType)
	}
}
