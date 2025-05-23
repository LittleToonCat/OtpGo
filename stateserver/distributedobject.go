package stateserver

import (
	"fmt"
	"otpgo/messagedirector"
	. "otpgo/util"
	"sort"
	"strings"
	"sync"

	"otpgo/dc"

	"github.com/apex/log"
)

type FieldValues map[dc.DCField][]byte

type DistributedObject struct {
	sync.Mutex
	messagedirector.MDParticipantBase

	log *log.Entry

	stateserver *StateServer
	do          Doid_t
	parent      Doid_t
	zone        Zone_t
	dclass      dc.DCClass

	requiredFields FieldValues
	ramFields      FieldValues

	aiChannel          Channel_t
	ownerChannel       Channel_t
	explicitAi         bool
	parentSynchronized bool

	zoneObjects map[Zone_t][]Doid_t
}

func NewDistributedObjectWithData(ss *StateServer, doid Doid_t, parent Doid_t,
	zone Zone_t, dclass dc.DCClass, requiredFields FieldValues,
	ramFields FieldValues) *DistributedObject {

	do := ss.doStore.createDO(ss, doid, dclass, requiredFields, ramFields)

	do.Init(do)
	do.SetName(fmt.Sprintf("%s (%d)", dclass.GetName(), doid))

	do.log.Debug("Object instantiated ...")

	do.SubscribeChannel(Channel_t(doid))
	do.Lock()
	do.handleLocationChange(parent, zone, 0)
	do.wakeChildren()
	do.Unlock()

	// if dgs, ok := messagedirector.ReplayPool[Channel_t(doid)]; ok {
	// 	for _, dg := range dgs {
	// 		dgi := NewDatagramIterator(&dg)
	// 		dgi.SeekPayload()
	// 		go do.HandleDatagram(dg, dgi)
	// 	}
	// }

	return do
}

func NewDistributedObject(ss *StateServer, doid Doid_t, parent Doid_t,
	zone Zone_t, dclass dc.DCClass, dgi *DatagramIterator, hasOther bool,
	isMainObj bool) (bool, *DistributedObject, error) {

	do := ss.doStore.createDO(ss, doid, dclass, nil, nil)

	DCLock.Lock()

	for i := 0; i < dclass.GetNumInheritedFields(); i++ {
		field := dclass.GetInheritedField(i)
		if field.IsRequired() {
			if molecular, ok := field.AsMolecularField().(dc.DCMolecularField); ok {
				if molecular != dc.SwigcptrDCMolecularField(0) {
					continue
				}
			}
			if data, ok := dgi.ReadDCField(field, true, false); ok {
				do.requiredFields[field] = data
				do.log.Debugf("Stored REQUIRED field \"%s\": %s", field.GetName(), FormatFieldData(field, do.requiredFields[field]))
			} else {
				return false, nil, fmt.Errorf("received truncated data for REQUIRED field \"%s\"\n%x", field.GetName(), data)
			}
		}
	}

	if hasOther {
		count := dgi.ReadUint16()
		for i := 0; i < int(count); i++ {
			id := dgi.ReadUint16()
			field := dclass.GetFieldByIndex(int(id))
			if field == dc.SwigcptrDCField(0) {
				do.log.Errorf("Receieved unknown field with ID %d within an OTHER section!  Ignoring.", id)
				break
			}

			if !field.IsRam() {
				do.log.Errorf("Received non-RAM field %s within an OTHER section!", field.GetName())
				dgi.SkipDCField(field, false)
				continue
			}
			if data, ok := dgi.ReadDCField(field, true, false); ok {
				do.ramFields[field] = data
				do.log.Debugf("Stored optional RAM field \"%s\": %s", field.GetName(), FormatFieldData(field, do.ramFields[field]))
			} else {
				return false, nil, fmt.Errorf("received truncated data for OTHER field \"%s\"\n%s", field.GetName(), dgi)
			}
		}
	}

	do.Init(do)
	do.SetName(fmt.Sprintf("%s (%d)", dclass.GetName(), doid))

	do.log.Debug("Object instantiated ...")

	DCLock.Unlock()

	if !isMainObj {
		do.SubscribeChannel(Channel_t(doid))
		do.Lock()
		dgi.SeekPayload()
		do.handleLocationChange(parent, zone, dgi.ReadChannel())
		do.wakeChildren()
		do.Unlock()
	}

	if strings.HasSuffix(dclass.GetName(), "District") {
		// It's a District object, automatically assign the airecv channel to the sender of the
		// generate message.
		dgi.SeekPayload()
		sender := dgi.ReadChannel()
		do.handleAiChange(sender, sender, true)
	}

	// Replay datagrams we may have missed while generating
	// if dgs, ok := messagedirector.ReplayPool[Channel_t(doid)]; ok {
	// 	for _, dg := range dgs {
	// 		dgi := NewDatagramIterator(&dg)
	// 		dgi.SeekPayload()
	// 		go do.HandleDatagram(dg, dgi)
	// 	}
	// }

	return true, do, nil
}

func (d *DistributedObject) appendRequiredData(dg Datagram, client bool) {
	dg.AddDoid(d.do)
	dg.AddLocation(d.parent, d.zone)
	dg.AddUint16(uint16(d.dclass.GetNumber()))
	count := d.dclass.GetNumInheritedFields()
	for i := 0; i < int(count); i++ {
		field := d.dclass.GetInheritedField(i)
		if molecular, ok := field.AsMolecularField().(dc.DCMolecularField); ok {
			if molecular != dc.SwigcptrDCMolecularField(0) {
				continue
			}
		}

		if field.IsRequired() && (!client || field.IsBroadcast() ||
			field.IsClrecv()) {
			dg.AddData(d.requiredFields[field])
		}
	}
}

func (d *DistributedObject) appendRequiredDataDoidLast(dg Datagram, client bool) {
	dg.AddLocation(d.parent, d.zone)
	dg.AddUint16(uint16(d.dclass.GetNumber()))
	dg.AddDoid(d.do)
	count := d.dclass.GetNumInheritedFields()
	for i := 0; i < int(count); i++ {
		field := d.dclass.GetInheritedField(i)
		if molecular, ok := field.AsMolecularField().(dc.DCMolecularField); ok {
			if molecular != dc.SwigcptrDCMolecularField(0) {
				continue
			}
		}

		if field.IsRequired() && (!client || field.IsBroadcast() ||
			field.IsClrecv()) {
			dg.AddData(d.requiredFields[field])
		}
	}
}

func (d *DistributedObject) appendOtherData(dg Datagram, client bool) {
	if client {
		var broadcastFields []dc.DCField
		for field := range d.ramFields {
			if field.IsBroadcast() || field.IsClrecv() {
				broadcastFields = append(broadcastFields, field)
			}
		}
		sort.Slice(broadcastFields, func(i, j int) bool {
			return broadcastFields[i].GetNumber() < broadcastFields[j].GetNumber()
		})

		dg.AddUint16(uint16(len(broadcastFields)))
		for _, field := range broadcastFields {
			dg.AddUint16(uint16(field.GetNumber()))
			dg.AddData(d.ramFields[field])
		}
	} else {
		var fields []dc.DCField
		for field := range d.ramFields {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].GetNumber() < fields[j].GetNumber()
		})

		dg.AddUint16(uint16(len(fields)))
		for _, field := range fields {
			dg.AddUint16(uint16(field.GetNumber()))
			dg.AddData(d.ramFields[field])
		}
	}
}

func (d *DistributedObject) sendInterestEntry(location Channel_t, context uint32) {
	msgType := STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED
	if len(d.ramFields) != 0 {
		msgType = STATESERVER_OBJECT_ENTER_INTEREST_WITH_REQUIRED_OTHER
	}
	dg := NewDatagram()
	dg.AddServerHeader(location, Channel_t(d.do), uint16(msgType))
	dg.AddUint32(context)
	d.appendRequiredData(dg, true)
	if len(d.ramFields) != 0 {
		d.appendOtherData(dg, true)
	}
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) sendLocationEntry(location Channel_t) {
	msgType := STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED
	if len(d.ramFields) != 0 {
		msgType = STATESERVER_OBJECT_ENTER_LOCATION_WITH_REQUIRED_OTHER
	}
	dg := NewDatagram()
	dg.AddServerHeader(location, Channel_t(d.do), uint16(msgType))
	d.appendRequiredData(dg, true)
	if len(d.ramFields) != 0 {
		d.appendOtherData(dg, true)
	}
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) sendAiEntry(ai Channel_t, sender Channel_t) {

	if ai == sender {
		// Do not relay the entry back to sender
		return
	}
	d.log.Debugf("Sending AI entry to %d", ai)
	msgType := STATESERVER_OBJECT_ENTER_AI_RECV
	dg := NewDatagram()
	dg.AddServerHeader(ai, Channel_t(d.do), uint16(msgType))
	dg.AddUint32(0) // Dummy context
	d.appendRequiredDataDoidLast(dg, false)
	d.appendOtherData(dg, false)
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) sendOwnerEntry(owner Channel_t, client bool) {
	msgType := STATESERVER_OBJECT_ENTER_OWNER_RECV
	dg := NewDatagram()
	dg.AddServerHeader(owner, Channel_t(d.do), uint16(msgType))
	// NOTE: We don't filter for ownrecv fields.  The receiver is expected to filter
	// that out itselves.  This is the case for original Panda3D message handling:
	// https://github.com/rocketprogrammer/panda3d/blob/otp-with-decompile/direct/src/dcparser/dcClass_ext.cxx#L162
	d.appendRequiredData(dg, client)
	d.appendOtherData(dg, client)
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) handleLocationChange(parent Doid_t, zone Zone_t, sender Channel_t) {
	var targets []Channel_t
	oldParent := d.parent
	oldZone := d.zone

	if d.ownerChannel != INVALID_CHANNEL {
		targets = append(targets, d.ownerChannel)
	}

	if parent == d.do {
		d.log.Warn("Object cannot be parented to itself.")
		return
	}

	// Parent change
	if parent != oldParent {
		if oldParent != INVALID_DOID {
			d.UnsubscribeChannel(ParentToChildren(d.parent))
			targets = append(targets, Channel_t(oldParent))
			targets = append(targets, LocationAsChannel(oldParent, oldZone))
		}

		d.parent = parent
		d.zone = zone

		if parent != INVALID_DOID {
			d.SubscribeChannel(ParentToChildren(parent))
			if !d.explicitAi {
				// Retrieve parent AI
				dg := NewDatagram()
				dg.AddServerHeader(Channel_t(parent), Channel_t(d.do), STATESERVER_OBJECT_GET_AI)
				// dg.AddUint32(d.context)
				// Send our sender as the context
				dg.AddUint32(uint32(sender)) // prob a bad idea to convert this but...
				d.RouteDatagramEarly(dg)
				// d.context++
			}
			targets = append(targets, Channel_t(parent))
		} else if !d.explicitAi {
			d.aiChannel = INVALID_CHANNEL
		}
	} else if zone != oldZone {
		d.zone = zone
		targets = append(targets, Channel_t(oldParent))
		targets = append(targets, LocationAsChannel(oldParent, oldZone))

		if d.aiChannel != INVALID_CHANNEL {
			targets = append(targets, d.aiChannel)
		}
	} else {
		return
	}

	// Broadcast location change message
	dg := NewDatagram()
	dg.AddMultipleServerHeader(targets, sender, STATESERVER_OBJECT_CHANGE_ZONE)
	dg.AddDoid(d.do)
	dg.AddLocation(parent, zone)
	dg.AddLocation(oldParent, oldZone)
	d.RouteDatagramEarly(dg)

	d.parentSynchronized = false

	if parent != INVALID_DOID {
		d.sendLocationEntry(LocationAsChannel(parent, zone))
	}
}

func (d *DistributedObject) handleAiChange(ai Channel_t, sender Channel_t, explicit bool) {
	d.log.Debugf("Changing AI channel to %d", ai)

	var targets []Channel_t
	oldAi := d.aiChannel
	if ai == oldAi {
		return
	}

	if oldAi != INVALID_CHANNEL {
		targets = append(targets, oldAi)
	}

	if len(d.zoneObjects) != 0 {
		// Notify children of the change
		targets = append(targets, ParentToChildren(d.do))
	}

	d.aiChannel = ai
	d.explicitAi = explicit

	if len(targets) > 0 {
		dg := NewDatagram()
		dg.AddMultipleServerHeader(targets, sender, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
		dg.AddDoid(d.do)
		dg.AddChannel(ai)
		dg.AddChannel(oldAi)
		d.RouteDatagramEarly(dg)
	}

	if ai != INVALID_CHANNEL {
		d.sendAiEntry(ai, sender)
	}
}

func (d *DistributedObject) annihilate(sender Channel_t, notifyParent bool) {
	var targets []Channel_t
	if d.parent != INVALID_DOID {
		targets = append(targets, LocationAsChannel(d.parent, d.zone))
		if notifyParent {
			dg := NewDatagram()
			dg.AddServerHeader(Channel_t(d.parent), sender, STATESERVER_OBJECT_CHANGE_ZONE)
			dg.AddDoid(d.do)
			dg.AddLocation(INVALID_DOID, 0)
			dg.AddLocation(d.parent, d.zone)
			d.RouteDatagramEarly(dg)
		}
	}

	if d.ownerChannel != INVALID_CHANNEL {
		targets = append(targets, d.ownerChannel)
	}

	if d.aiChannel != INVALID_CHANNEL {
		dg := NewDatagram()
		dg.AddServerHeader(d.aiChannel, sender, STATESERVER_OBJECT_LEAVING_AI_INTEREST)
		dg.AddDoid(d.do)
		d.RouteDatagramEarly(dg)
	}

	dg := NewDatagram()
	dg.AddMultipleServerHeader(targets, sender, STATESERVER_OBJECT_DELETE_RAM)
	dg.AddDoid(d.do)
	d.RouteDatagramEarly(dg)

	d.deleteChildren(sender)
	delete(d.stateserver.objects, d.do)
	d.log.Debug("Deleted object.")

	clear(d.zoneObjects)
	clear(d.requiredFields)
	clear(d.ramFields)

	d.Cleanup()
	d.stateserver.doStore.recycleDO(d)
}

func (d *DistributedObject) deleteChildren(sender Channel_t) {
	if len(d.zoneObjects) != 0 {
		dg := NewDatagram()
		dg.AddServerHeader(ParentToChildren(d.do), sender, STATESERVER_OBJECT_DELETE_CHILDREN)
		dg.AddDoid(d.do)
		d.RouteDatagramEarly(dg)
	}
}

func (d *DistributedObject) wakeChildren() {
	dg := NewDatagram()
	dg.AddServerHeader(ParentToChildren(d.do), Channel_t(d.do), STATESERVER_OBJECT_LOCATE)
	dg.AddUint32(STATESERVER_CONTEXT_WAKE_CHILDREN)
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) saveField(field dc.DCField, data []byte) bool {
	if field.IsDb() && d.stateserver.database != INVALID_CHANNEL {
		d.log.Debugf("Forwarding update for field \"%s\": %s of object id %d to database.", field.GetName(), FormatFieldData(field, data), d.do)

		dg := NewDatagram()
		dg.AddServerHeader(d.stateserver.database, Channel_t(d.do), DBSERVER_SET_STORED_VALUES)
		dg.AddDoid(d.do)
		dg.AddUint16(1) // Field count
		dg.AddString(field.GetName())
		dg.AddUint16(uint16(len(data)))
		dg.AddData(data)

		d.RouteDatagramEarly(dg)
	}

	if field.IsRequired() {
		d.log.Debugf("Storing REQUIRED field \"%s\": %s", field.GetName(), FormatFieldData(field, data))
		d.requiredFields[field] = data
		return true
	} else if field.IsRam() {
		d.log.Debugf("Storing RAM field \"%s\": %s", field.GetName(), FormatFieldData(field, data))
		d.ramFields[field] = data
		return true
	}
	return false
}

func (d *DistributedObject) handleOneUpdate(dgi *DatagramIterator, sender Channel_t) bool {
	fieldId := dgi.ReadUint16()
	field := d.dclass.GetFieldByIndex(int(fieldId))
	if field == dc.SwigcptrDCField(0) {
		d.log.Warnf("Update received for unknown field ID=%d", fieldId)
		return false
	}

	offset := dgi.Tell()
	data, ok := dgi.ReadDCField(field, true, true)
	if !ok || dgi.RemainingSize() > 0 {
		dgi.Seek(offset)
		d.log.Errorf("Received invalid update data for field \"%s\"!\n%s\n%x", field.GetName(), dgi, dgi.ReadRemainder())
	}

	// Hand things over to finishHandleUpdate
	d.finishHandleUpdate(field, data, sender)
	return true
}

func (d *DistributedObject) handleMultipleUpdates(dgi *DatagramIterator, count uint16, sender Channel_t) bool {
	for i := 0; i < int(count); i++ {
		fieldId := dgi.ReadUint16()
		field := d.dclass.GetFieldByIndex(int(fieldId))
		if field == dc.SwigcptrDCField(0) {
			d.log.Warnf("Update received for unknown field ID=%d", fieldId)
			return false
		}

		offset := dgi.Tell()
		data, ok := dgi.ReadDCField(field, true, true)
		if !ok {
			dgi.Seek(offset)
			d.log.Errorf("Received invalid update data for field \"%s\"!\n%s\n%x", field.GetName(), dgi, dgi.ReadRemainder())
			return false
		}
		d.finishHandleUpdate(field, data, sender)
	}

	return true
}

func (d *DistributedObject) finishHandleUpdate(field dc.DCField, data []byte, sender Channel_t) {
	DCLock.Lock()
	defer DCLock.Unlock()
	// Print out the human formatted data
	d.log.Debugf("Handling update for field \"%s\": %s", field.GetName(), FormatFieldData(field, data))

	molecular := field.AsMolecularField().(dc.DCMolecularField)
	if molecular != dc.SwigcptrDCMolecularField(0) {
		// Time to pull out a DatagramIterator for this one.
		dg := NewDatagram()
		dg.AddData(data)
		dgi := NewDatagramIterator(&dg)

		count := molecular.GetNumAtomics()
		for n := 0; n < count; n++ {
			atomic := molecular.GetAtomic(n).AsField().(dc.DCField)
			atomicData, ok := dgi.ReadDCField(atomic, true, false)
			if !ok {
				d.log.Errorf("Failed to read atomic field \"%s\" of molecular field \"%s\".", atomic.GetName(), molecular.GetName())
				return
			}
			// We save atomic fields seperately, not whole moleculars.
			d.saveField(atomic, atomicData)
		}
	} else {
		d.saveField(field, data)
	}

	var targets []Channel_t
	if field.IsBroadcast() {
		targets = append(targets, LocationAsChannel(d.parent, d.zone))
	}

	if field.IsAirecv() && d.aiChannel != INVALID_CHANNEL && d.aiChannel != sender {
		targets = append(targets, d.aiChannel)
	}

	if field.IsOwnrecv() && d.ownerChannel != INVALID_CHANNEL && d.ownerChannel != sender {
		targets = append(targets, d.ownerChannel)
	}

	if len(targets) != 0 {
		dg := NewDatagram()
		dg.AddMultipleServerHeader(targets, sender, STATESERVER_OBJECT_UPDATE_FIELD)
		dg.AddDoid(d.do)
		dg.AddUint16(uint16(field.GetNumber()))
		dg.AddData(data)
		d.RouteDatagramEarly(dg)
	}
}

func (d *DistributedObject) handleOneGet(out *Datagram, fieldId uint16, allowUnset bool, subfield bool) bool {
	field := d.dclass.GetFieldByIndex(int(fieldId))
	if field == dc.SwigcptrDCField(0) {
		d.log.Warnf("Query received for unknown field ID=%d", fieldId)
		return false
	}

	d.log.Debugf("Handling query for field %s", field.GetName())
	molecular := field.AsMolecularField().(dc.DCMolecularField)
	if molecular != dc.SwigcptrDCMolecularField(0) {
		count := molecular.GetNumAtomics()
		out.AddUint16(fieldId)
		for n := 0; n < count; n++ {
			if !d.handleOneGet(out, uint16(molecular.GetAtomic(n).GetNumber()), allowUnset, true) {
				return false
			}
		}
		return true
	}

	if data, ok := d.requiredFields[field]; ok {
		if !subfield {
			out.AddUint16(fieldId)
		}
		out.AddData(data)
	} else if data, ok := d.ramFields[field]; ok {
		if !subfield {
			out.AddUint16(fieldId)
		}
		out.AddData(data)
	} else {
		return allowUnset
	}

	return true
}

func (d *DistributedObject) handleQueryAll(sender Channel_t, context uint32) {
	dg := NewDatagram()
	dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_QUERY_OBJECT_ALL_RESP)
	dg.AddUint32(context)
	d.appendRequiredDataDoidLast(dg, false)
	d.appendOtherData(dg, false)
	d.RouteDatagramEarly(dg)
}

func (d *DistributedObject) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	d.Lock()
	defer d.Unlock()

	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				d.log.Errorf("Received truncated datagram")
			}
		}
	}()

	sender := dgi.ReadChannel()
	msgType := dgi.ReadUint16()

	switch msgType {
	case STATESERVER_SHARD_REST:
		if d.aiChannel != dgi.ReadChannel() {
			d.log.Warnf("Received reset for wrong AI channel!")
			return
		}

		d.annihilate(sender, true)
	case STATESERVER_OBJECT_DELETE_RAM:
		if d.do != dgi.ReadDoid() {
			break
		}

		d.annihilate(sender, true)
	case STATESERVER_OBJECT_DELETE_CHILDREN:
		do := dgi.ReadDoid()
		if d.do == do {
			d.deleteChildren(sender)
		} else if do == d.parent {
			d.annihilate(sender, false)
		}
	case STATESERVER_OBJECT_UPDATE_FIELD:
		if d.do != dgi.ReadDoid() {
			break
		}

		d.handleOneUpdate(dgi, sender)
	case STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE:
		if d.do != dgi.ReadDoid() {
			break
		}

		count := dgi.ReadUint16()
		d.handleMultipleUpdates(dgi, count, sender)
	case STATESERVER_OBJECT_LEAVING_AI_INTEREST:
		parent := dgi.ReadDoid()
		newChannel := dgi.ReadChannel()
		d.log.Debugf("Received changing AI message from %d", parent)
		if parent != d.parent {
			d.log.Warnf("Received changing AI message from %d, but my parent is %d", parent, d.parent)
			return
		}
		if d.explicitAi {
			break
		}
		d.handleAiChange(newChannel, sender, false)
	case STATESERVER_ADD_AI_RECV:
		newChannel := dgi.ReadChannel()
		d.handleAiChange(newChannel, sender, true)
	case STATESERVER_OBJECT_GET_AI:
		// d.log.Debugf("Received AI query from %d", sender)
		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_OBJECT_GET_AI_RESP)
		dg.AddUint32(dgi.ReadUint32()) // Context
		dg.AddDoid(d.do)
		dg.AddChannel(d.aiChannel)
		d.RouteDatagramEarly(dg)
	case STATESERVER_OBJECT_GET_AI_RESP:
		context := dgi.ReadUint32()
		parent := dgi.ReadDoid()
		d.log.Debugf("Received AI query response from %d", parent)
		if parent != d.parent {
			d.log.Warnf("Received AI channel from %d, but parent is %d", parent, d.parent)
			return
		}

		ai := dgi.ReadChannel()
		if d.explicitAi {
			return
		}
		d.handleAiChange(ai, Channel_t(context), false)
	case STATESERVER_OBJECT_CHANGE_ZONE:
		child := dgi.ReadDoid()
		newParent := dgi.ReadDoid()
		newZone := dgi.ReadZone()
		oldParent := dgi.ReadDoid()
		oldZone := dgi.ReadZone()
		eraseFromSlice := func(slice []Doid_t, element Doid_t) []Doid_t {
			tempSlice := make([]Doid_t, 0)
			for _, do := range slice {
				if do != element {
					tempSlice = append(tempSlice, do)
				}
			}
			return tempSlice
		}
		if newParent == d.do {
			if d.do == oldParent {
				if newZone == oldZone {
					return // No change
				}
				d.zoneObjects[oldZone] = eraseFromSlice(d.zoneObjects[oldZone], child)
				if len(d.zoneObjects[oldZone]) == 0 {
					delete(d.zoneObjects, oldZone)
				}
			}

			alreadyContains := false
			if slice, ok := d.zoneObjects[newZone]; ok {
				for _, zoneDo := range slice {
					if child == zoneDo {
						alreadyContains = true
						break
					}
				}
			}
			if alreadyContains {
				d.log.Debugf("STATESERVER_OBJECT_CHANGE_ZONE: zoneObjects[%d] already contains %d!", newZone, child)
			} else {
				d.zoneObjects[newZone] = append(d.zoneObjects[newZone], child)
			}

			dg := NewDatagram()
			dg.AddServerHeader(Channel_t(child), Channel_t(d.do), STATESERVER_OBJECT_LOCATION_ACK)
			dg.AddDoid(d.do)
			dg.AddZone(newZone)
			d.RouteDatagramEarly(dg)
		} else if oldParent == d.do {
			d.zoneObjects[oldZone] = eraseFromSlice(d.zoneObjects[oldZone], child)
			if len(d.zoneObjects[oldZone]) == 0 {
				delete(d.zoneObjects, oldZone)
			}
		} else {
			d.log.Warnf("Received changing location from %d for %d, but my id is %d", child, oldParent, d.do)
		}
	case STATESERVER_OBJECT_LOCATION_ACK:
		parent := dgi.ReadDoid()
		zone := dgi.ReadZone()
		if parent != d.parent {
			d.log.Debugf("Received location acknowledgement from %d but my parent is %d!", parent, d.parent)
		} else if zone != d.zone {
			d.log.Debugf("Received location acknowledgement for zone %d but my zone is %d!", zone, d.zone)
		} else {
			d.log.Debugf("Parent acknowledged my location change!")
			d.parentSynchronized = true
		}
	case STATESERVER_OBJECT_SET_ZONE:
		newParent := dgi.ReadDoid()
		newZone := dgi.ReadZone()
		d.log.Debugf("Updating location; parent=%d, zone=%d", newParent, newZone)
		d.handleLocationChange(newParent, newZone, sender)
	case STATESERVER_OBJECT_LOCATE:
		context := dgi.ReadUint32()

		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_OBJECT_LOCATE_RESP)
		dg.AddUint32(context)
		dg.AddDoid(d.do)
		dg.AddLocation(d.parent, d.zone)
		d.RouteDatagramEarly(dg)
	case STATESERVER_OBJECT_LOCATE_RESP:
		if dgi.ReadUint32() != STATESERVER_CONTEXT_WAKE_CHILDREN {
			d.log.Warnf("Received unexpected GET_LOCATION_RESP from %d", dgi.ReadUint32())
			return
		}

		do := dgi.ReadDoid()
		parent := dgi.ReadDoid()
		zone := dgi.ReadZone()

		if parent == d.do {
			if slice, ok := d.zoneObjects[zone]; ok {
				for _, zoneDo := range slice {
					if do == zoneDo {
						d.log.Debugf("STATESERVER_OBJECT_LOCATE_RESP: zoneObjects[%d] already contains %d!", zone, do)
						return
					}
				}
			}
			d.zoneObjects[zone] = append(d.zoneObjects[zone], do)
		}
	case STATESERVER_QUERY_OBJECT_ALL:
		d.handleQueryAll(sender, dgi.ReadUint32())
	case STATESERVER_OBJECT_QUERY_FIELD:
		if dgi.ReadDoid() != d.do {
			return
		}

		fieldId := dgi.ReadUint16()

		context := dgi.ReadUint32()

		field := NewDatagram()
		success := d.handleOneGet(&field, fieldId, false, true)

		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_OBJECT_QUERY_FIELD_RESP)
		dg.AddDoid(d.do)
		dg.AddUint16(fieldId)
		dg.AddUint32(context)
		dg.AddBool(success)
		if success {
			dg.AddDatagram(&field)
		}
		d.RouteDatagramEarly(dg)
	case STATESERVER_OBJECT_QUERY_FIELDS:
		if dgi.ReadDoid() != d.do {
			return
		}
		context := dgi.ReadUint32()

		var requestedFields []uint16
		for dgi.RemainingSize() >= Blobsize {
			fieldId := dgi.ReadUint16()
			requestedFields = append(requestedFields, fieldId)
		}
		sort.Slice(requestedFields, func(i, j int) bool {
			return requestedFields[i] < requestedFields[j]
		})

		success, found, fields := true, 0, NewDatagram()
		for _, fieldId := range requestedFields {
			sz := fields.Len()
			if !d.handleOneGet(&fields, fieldId, true, false) {
				success = false
				break
			}
			if fields.Len() > sz {
				found++
			}
		}

		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_OBJECT_QUERY_FIELDS_RESP)
		dg.AddDoid(d.do)
		dg.AddUint32(context)
		dg.AddBool(success)
		if success {
			dg.AddUint16(uint16(found))
			dg.AddDatagram(&fields)
		}
		d.RouteDatagramEarly(dg)
	case STATESERVER_OBJECT_SET_OWNER_RECV:
		fallthrough
	case STATESERVER_OBJECT_SET_OWNER_RECV_WITH_ALL:
		newOwner := dgi.ReadChannel()
		if newOwner == d.ownerChannel {
			d.log.Debugf("Received owner change, but owner is the same.")
			return
		} else {
			d.log.Debugf("Owner changing to %d!", newOwner)
		}

		if d.ownerChannel != INVALID_CHANNEL {
			dg := NewDatagram()
			dg.AddServerHeader(d.ownerChannel, sender, STATESERVER_OBJECT_CHANGE_OWNER_RECV)
			dg.AddDoid(d.do)
			dg.AddChannel(newOwner)
			dg.AddChannel(d.ownerChannel)
			d.RouteDatagramEarly(dg)
		}

		d.ownerChannel = newOwner

		if newOwner != INVALID_CHANNEL {
			d.sendOwnerEntry(newOwner, msgType == STATESERVER_OBJECT_SET_OWNER_RECV)
		}
	case STATESERVER_OBJECT_GET_ZONE_OBJECTS:
		fallthrough
	case STATESERVER_OBJECT_GET_ZONES_OBJECTS:
		context := dgi.ReadUint32()
		queriedParent := dgi.ReadDoid()

		d.log.Debugf("Handling GET_ZONES_OBJECTS; queried parent=%d, id=%d, parent=%d", queriedParent, d.do, d.parent)

		zoneCount := 1
		if msgType == STATESERVER_OBJECT_GET_ZONES_OBJECTS {
			zoneCount = int(dgi.ReadUint16())
		}

		if queriedParent == d.parent {
			// Query was relayed from our parent
			for n := 0; n < zoneCount; n++ {
				if dgi.ReadZone() == d.zone {
					// If you're actually reading through this code, please look through
					//  the comments in Astron C++ to understand what is going on; most of
					//  this code is a transposition of Astron C++.
					if d.parentSynchronized {
						d.sendInterestEntry(sender, context)
					} else {
						d.sendLocationEntry(sender)
					}
					break
				}
			}
		} else if queriedParent == d.do {
			childCount := 0

			dg := NewDatagram()
			dg.AddServerHeader(ParentToChildren(d.do), sender, STATESERVER_OBJECT_GET_ZONES_OBJECTS)
			dg.AddUint32(context)
			dg.AddDoid(queriedParent)
			dg.AddUint16(uint16(zoneCount))

			for n := 0; n < zoneCount; n++ {
				zone := dgi.ReadZone()
				childCount += len(d.zoneObjects[zone])
				dg.AddZone(zone)
			}

			countDg := NewDatagram()
			countDg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_OBJECT_GET_ZONES_COUNT_RESP)
			countDg.AddUint32(context)
			countDg.AddDoid(Doid_t(childCount))
			d.RouteDatagramEarly(countDg)

			if childCount > 0 {
				d.RouteDatagramEarly(dg)
			}
		}
	case STATESERVER_GET_ACTIVE_ZONES:
		var zones []Zone_t
		context := dgi.ReadUint32()

		for zone := range d.zoneObjects {
			zones = append(zones, zone)
		}
		sort.Slice(zones, func(i, j int) bool {
			return zones[i] < zones[j]
		})

		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(d.do), STATESERVER_GET_ACTIVE_ZONES_RESP)
		dg.AddUint32(context)
		dg.AddUint16(uint16(len(zones)))

		for _, zone := range zones {
			dg.AddZone(zone)
		}

		d.RouteDatagramEarly(dg)
	default:
		if msgType < STATESERVER_MSGTYPE_MIN || msgType > STATESERVER_MSGTYPE_MAX {
			d.log.Warnf("Recieved unknown message of type %d.", msgType)
		} else {
			d.log.Warnf("Ignoring message of type %d.", msgType)
		}

	}
}
