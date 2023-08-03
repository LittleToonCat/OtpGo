package stateserver

import (
	"otpgo/core"
	"fmt"
	. "otpgo/util"

	dc "github.com/LittleToonCat/dcparser-go"
)

type LoadingObject struct {
	dbss     *DatabaseStateServer
	do       Doid_t
	parent   Doid_t
	zone     Zone_t
	dclass   dc.DCClass

	requiredFields FieldValues
	ramFields      FieldValues

	fieldUpdates   FieldValues

	context  uint32
	dgQueue  []Datagram
}

type DatabaseStateServer struct {
	StateServer

	database Channel_t
	loading map[Doid_t]LoadingObject
	context uint32
	contextToLoading map[uint32]LoadingObject
}

func NewDatabaseStateServer(config core.Role) *DatabaseStateServer {
	dbss := &DatabaseStateServer{
		database: config.Database,
		loading: map[Doid_t]LoadingObject{},
		context: 0,
		contextToLoading: map[uint32]LoadingObject{},
	}
	dbss.InitStateServer(config, fmt.Sprintf("DBSS (%d - %d)", dbss.config.Ranges.Min, dbss.config.Ranges.Max))

	dbss.Init(dbss)

	dbss.SubscribeRange(dbss.config.Ranges)

	return dbss
}

func (s *DatabaseStateServer) handleActivate(dgi *DatagramIterator, other bool) {
	do := dgi.ReadDoid()
	parent := dgi.ReadDoid()
	zone := dgi.ReadZone()

	if _, ok := s.objects[do]; ok {
		s.log.Warnf("Received activate for already-active object with id %d", do)
		return
	} else if _, ok := s.loading[do]; ok {
		s.log.Warnf("Received activate for already-loading object with id %d", do)
		return
	}

	dcId := dgi.ReadUint16()
	if core.DC.Get_num_classes() < int(dcId) {
		s.log.Errorf("Received activate for unknown dclass id %d", dcId)
		return
	}
	dclass := core.DC.Get_class(int(dcId))

	obj := LoadingObject{
		dbss: s,
		do: do,
		parent: parent,
		zone: zone,
		dclass: dclass,

		requiredFields: FieldValues{},
		ramFields: FieldValues{},

		fieldUpdates: FieldValues{},

		context: s.context,
	}

	if other {
		count := dgi.ReadUint16()

		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		remainder := dgi.ReadRemainderAsVector()
		defer dc.DeleteVector_uchar(remainder)
		unpacker.Set_unpack_data(remainder)

		for i := uint16(0); i < count; i++ {
			field := unpacker.Raw_unpack_uint16().(uint)
			dcField := dclass.Get_inherited_field(int(field))
			if dcField == dc.SwigcptrDCField(0) {
				s.log.Errorf("Received invalid field index %d", field)
				return
			}

			unpacker.Begin_unpack(dcField)
			if !(dcField.Is_required() || dcField.Is_ram()) {
				s.log.Errorf("Recieved NON-RAM field \"%s\" within an OTHER section", dcField.Get_name())
				unpacker.Unpack_skip()
				unpacker.End_unpack()
				continue
			}
			packedData := unpacker.Unpack_literal_value().(dc.Vector_uchar)
			if !unpacker.End_unpack() {
				s.log.Errorf("Received invalid update data for field \"%s\"!", dcField.Get_name())
				dc.DeleteVector_uchar(packedData)
				continue
			}

			obj.fieldUpdates[dcField] = packedData
		}
	}

	s.loading[do] = obj
	s.contextToLoading[s.context] = obj

	// Populate names of required fields to fetch.
	required := make([]string, 0)
	count := dclass.Get_num_inherited_fields()
	for i := 0; i < count; i++ {
		field := dclass.Get_inherited_field(i)
		molecular := field.As_molecular_field().(dc.DCMolecularField)
		if (molecular != dc.SwigcptrDCMolecularField(0)) {
			continue
		}
		if field.Is_required() && field.Is_db() {
			if _, ok := obj.fieldUpdates[field]; !ok {
				required = append(required, field.Get_name())
			}
		}
	}

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(s.context)
	dg.AddDoid(do)
	dg.AddUint16(uint16(len(required)))
	for _, field := range required {
		dg.AddString(field)
	}
	s.RouteDatagram(dg)

	s.context++
}

func (s * DatabaseStateServer) handleGetStoredValues(dgi *DatagramIterator) {
	context := dgi.ReadUint32()
	obj, ok := s.contextToLoading[context]
	if !ok {
		s.log.Warnf("Received context %d but no LoadingObject found!", context)
		return
	}
	delete(s.contextToLoading, context)

	do := dgi.ReadDoid()
	if obj.do != do {
		s.log.Warnf("Received GetStoredValues for wrong DOID! %d != %d", obj.do, do)
		s.finalizeLoading(&obj)
		return
	}

	count := dgi.ReadUint16()
	fields := make([]string, count)
	for i := uint16(0); i < count; i++ {
		fields[i] = dgi.ReadString()
	}

	code := dgi.ReadUint8()
	if code > 0 {
		if code == 1 {
			s.log.Errorf("Object %d not found in database.", do)
		} else {
			s.log.Errorf("GetStoredValues failed for DOID %d", do)
		}

		s.finalizeLoading(&obj)
		return
	}

	packedValues := make([]dc.Vector_uchar, count)
	hasValue := map[string]bool{}
	for i := uint16(0); i < count; i++ {
		packedValues[i] = dgi.ReadVector()
		hasValue[fields[i]] = dgi.ReadBool()
		if !hasValue[fields[i]] {
			s.log.Debugf("Data for field \"%s\" not found", fields[i])
		}
	}

	// Whew, now that we've finally read the data, let's parse it.
	for i := uint16(0); i < count; i++ {
		field := fields[i]
		found := hasValue[field]

		dcField := obj.dclass.Get_field_by_name(field)
		if dcField == dc.SwigcptrDCField(0) {
			s.log.Warnf("Field \"%s\" does not exist for class \"%s\"", field, obj.dclass.Get_name())
			if found {
				dc.DeleteVector_uchar(packedValues[i])
			}
			continue
		}

		if !(dcField.Is_required() || dcField.Is_ram()) {
			s.log.Errorf("Recieved NON-RAM field \"%s\"", field)
			if found {
				dc.DeleteVector_uchar(packedValues[i])
			}
			continue
		}

		if _, ok := obj.fieldUpdates[dcField]; ok {
			// This has already been overridden by the
			// activate_other message earlier, so ignore.
			dc.DeleteVector_uchar(packedValues[i])
			continue
		}

		if found {
			data := packedValues[i]
			// Validate that the data is correct
			if !dcField.Validate_ranges(data) {
				s.log.Errorf("Received invalid update data for field \"%s\"!", field)
				dc.DeleteVector_uchar(data)
				continue
			}
			s.log.Debugf("Got data for field \"%s\": %s", fields[i], dcField.Format_data(data))
			obj.fieldUpdates[dcField] = data
		} else {
			s.log.Debugf("Data for field \"%s\" not found", fields[i])
			continue
		}
	}

	// Now let's get the object inited.
	numFields := obj.dclass.Get_num_inherited_fields()
	for i := 0; i < numFields; i++ {
		dcField := obj.dclass.Get_inherited_field(i)
		molecular := dcField.As_molecular_field().(dc.DCMolecularField)
		if (molecular != dc.SwigcptrDCMolecularField(0)) {
			continue
		}
	if dcField.Is_required() {
		if data, ok := obj.fieldUpdates[dcField]; ok {
			obj.requiredFields[dcField] = data
			delete(obj.fieldUpdates, dcField)
		} else {
			// Use the default value.

			// HACK: Because Get_default_value returns a pointer which will
			// become lost when accidentally deleted, we'd have to copy it.
			// into a new blob instance.
			value := dcField.Get_default_value()

			obj.requiredFields[dcField] = dc.NewVector_uchar()
			for i := int64(0); i < value.Size(); i++ {
				obj.requiredFields[dcField].Add(value.Get(int(i)))
			}

			s.log.Debugf("Using default value required for field \"%s\" %s", dcField.Get_name(), dcField.Format_data(obj.requiredFields[dcField]))
		}
	} else if dcField.Is_ram() {
		if data, ok := obj.fieldUpdates[dcField]; ok {
			obj.ramFields[dcField] = data
			delete(obj.fieldUpdates, dcField)
		}
	}
}

	dobj := s.CreateDistributedObjectWithData(obj.do, obj.parent, obj.zone, obj.dclass,
		obj.requiredFields, obj.ramFields)

	// Replay the datagrams to the object
	for _, dg := range obj.dgQueue {
		dgi := NewDatagramIterator(&dg)
		dgi.SeekPayload()
		dobj.HandleDatagram(dg, dgi)
	}

	// Clean up the unused vectors
	for field, data := range obj.fieldUpdates {
		dc.DeleteVector_uchar(data)
		delete(obj.fieldUpdates, field)
	}

	s.finalizeLoading(&obj)
}

func (s *DatabaseStateServer) finalizeLoading(obj *LoadingObject) {
	if _, ok := s.loading[obj.do]; ok {
		// Forward the datagrams to the DBSS
		for _, dg := range obj.dgQueue {
			dgi := NewDatagramIterator(&dg)
			dgi.SeekPayload()
			s.HandleDatagram(dg, dgi)
		}
		delete(s.loading, obj.do)
	}

}

func (s *DatabaseStateServer) handleOneUpdate(dgi *DatagramIterator)  {
	do := dgi.ReadDoid()
	if obj, ok := s.loading[do]; ok {
		// Add to the queue and leave it alone.  It'll be bounced back
		// when finished.
		obj.dgQueue = append(obj.dgQueue, *dgi.Dg)
		return
	}

	fieldId := dgi.ReadUint16()
	field := core.DC.Get_field_by_index(int(fieldId))
	if field == dc.SwigcptrDCField(0) {
		s.log.Warnf("Update received for unknown field ID=%d", fieldId)
	}

	if !field.Is_db() {
		// Ignore it.
		return
	}

	packedData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector_uchar(packedData)

	// Instead of constructing our DCPacker, let's call the field's Validate_ranges
	// method instead which does the job of validating the data for us.
	// Also checks for no extra bytes.
	if !field.Validate_ranges(packedData) {
		s.log.Errorf("Received invalid update data for field \"%s\"!", field.Get_name())
	}

	s.log.Debugf("Forwarding update for field \"%s\" of object id %d to database.", field.Get_name(), do)

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(do)
	dg.AddUint16(1) // Field count
	dg.AddString(field.Get_name())
	dg.AddUint16(uint16(packedData.Size()))
	dg.AddVector(packedData)

	s.RouteDatagram(dg)
}

func (s *DatabaseStateServer) handleMultipleUpdates(dgi *DatagramIterator) {
	do := dgi.ReadDoid()
	if obj, ok := s.loading[do]; ok {
		// Add to the queue and leave it alone.  It'll be bounced back
		// when finished.
		obj.dgQueue = append(obj.dgQueue, *dgi.Dg)
		return
	}

	count := dgi.ReadUint16()

	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	remainder := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector_uchar(remainder)
	unpacker.Set_unpack_data(remainder)

	fieldUpdates := map[string]dc.Vector_uchar{}

	for i := 0; i < int(count); i++ {
		fieldId := unpacker.Raw_unpack_uint16().(uint)
		field := core.DC.Get_field_by_index(int(fieldId))
		if field == dc.SwigcptrDCField(0) {
			s.log.Warnf("Update received for unknown field ID=%d", fieldId)
			return
		}

		if !field.Is_db() {
			// Skip the data.
			unpacker.Unpack_skip()
			// ..and even that could fail.
			if !unpacker.End_unpack() {
				s.log.Errorf("Received invalid update data for field \"%s\"!", field.Get_name())
				return
			}
			continue
		}

		unpacker.Begin_unpack(field)
		packedData := unpacker.Unpack_literal_value().(dc.Vector_uchar)
		defer dc.DeleteVector_uchar(packedData)
		if !unpacker.End_unpack() {
			s.log.Errorf("Received invalid update data for field \"%s\"!", field.Get_name())
			return
		}

		fieldUpdates[field.Get_name()] = packedData
	}

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(do)
	dg.AddUint16(uint16(len(fieldUpdates)))
	for field, data := range fieldUpdates {
		s.log.Debugf("Forwarding update for field \"%s\" of object id %d to database.", field, do)

		dg.AddString(field)
		dg.AddUint16(uint16(data.Size()))
		dg.AddVector(data)
	}

	s.RouteDatagram(dg)

}

func (s *DatabaseStateServer) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(DatagramIteratorEOF); ok {
				s.log.Errorf("Received truncated datagram")
			}
		}
	}()

	// Go back and get the sent channels, we need them.
	dgi.Seek(0)
	var receivers []Channel_t
	chanCount := dgi.ReadUint8()
	for n := 0; uint8(n) < chanCount; n++ {
		receivers = append(receivers, dgi.ReadChannel())
	}

	sender := dgi.ReadChannel()
	msgType := dgi.ReadUint16()

	switch msgType {
	case DBSERVER_GET_STORED_VALUES_RESP:
		s.handleGetStoredValues(dgi)
	// Accept regular SS generate messages.
	case STATESERVER_OBJECT_GENERATE_WITH_REQUIRED:
		s.handleGenerate(dgi, false)
	case STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
		s.handleGenerate(dgi, true)
	case STATESERVER_OBJECT_UPDATE_FIELD:
		s.handleOneUpdate(dgi)
	case STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE:
		s.handleMultipleUpdates(dgi)
	case DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS:
		s.handleActivate(dgi, false)
	case DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS_OTHER:
		s.handleActivate(dgi, true)
	case DBSS_OBJECT_GET_ACTIVATED:
		context := dgi.ReadUint32()
		doId := dgi.ReadDoid()

		_, ok := s.objects[doId]
		dg := NewDatagram()
		dg.AddServerHeader(sender, Channel_t(doId), DBSS_OBJECT_GET_ACTIVATED_RESP)
		dg.AddUint32(context)
		dg.AddDoid(doId)
		dg.AddBool(ok)
		s.RouteDatagram(dg)
	default:
		// Store it in the loading object datagram queue.
		for _, receiver := range receivers {
			if obj, ok := s.loading[Doid_t(receiver)]; ok {
				obj.dgQueue = append(obj.dgQueue, *dgi.Dg)
			}
		}
		s.log.Debugf("Ignoring message of type=%d", msgType)
	}
}
