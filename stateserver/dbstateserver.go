package stateserver

import (
	"fmt"
	"otpgo/core"
	. "otpgo/util"

	dc "github.com/LittleToonCat/dcparser-go"
)

type LoadingObject struct {
	dbss   *DatabaseStateServer
	do     Doid_t
	parent Doid_t
	zone   Zone_t
	dclass dc.DCClass

	requiredFields FieldValues
	ramFields      FieldValues

	fieldUpdates FieldValues

	context uint32
	dgQueue []Datagram

	queryAllFrom    Channel_t
	queryAllContext uint32
}

type FieldQuery struct {
	do      Doid_t
	from    Channel_t
	context uint32

	multiple      bool
	singleFieldId uint16
	name2FieldId  map[string]uint16
}

type DClassQuery struct {
	do      Doid_t
	from    Channel_t
	context uint32
	dg      Datagram
}

type DatabaseStateServer struct {
	StateServer

	database             Channel_t
	loading              map[Doid_t]*LoadingObject
	context              uint32
	contextToLoading     map[uint32]*LoadingObject
	contextToFieldQuery  map[uint32]*FieldQuery
	contextToQueryDClass map[uint32]*DClassQuery
	contextToQueryAll    map[uint32]*LoadingObject
}

func NewDatabaseStateServer(config core.Role) *DatabaseStateServer {
	dbss := &DatabaseStateServer{
		database:             config.Database,
		loading:              map[Doid_t]*LoadingObject{},
		context:              0,
		contextToLoading:     map[uint32]*LoadingObject{},
		contextToFieldQuery:  map[uint32]*FieldQuery{},
		contextToQueryDClass: map[uint32]*DClassQuery{},
		contextToQueryAll:    map[uint32]*LoadingObject{},
	}
	dbss.InitStateServer(config, fmt.Sprintf("DBSS (%d - %d)", dbss.config.Ranges.Min, dbss.config.Ranges.Max), "DBSS", "*")

	dbss.Init(dbss)
	dbss.SetName(fmt.Sprintf("DBSS (%d - %d)", dbss.config.Ranges.Min, dbss.config.Ranges.Max))

	dbss.SubscribeRange(dbss.config.Ranges)

	return dbss
}

func (s *DatabaseStateServer) handleActivate(dgi *DatagramIterator, other bool) {
	do := dgi.ReadDoid()
	parent := dgi.ReadDoid()
	zone := dgi.ReadZone()

	s.log.Debugf("Received activate for object=%d, other=%t", do, other)

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
		dbss:   s,
		do:     do,
		parent: parent,
		zone:   zone,
		dclass: dclass,

		requiredFields: FieldValues{},
		ramFields:      FieldValues{},

		fieldUpdates: FieldValues{},

		context: s.context,
		dgQueue: []Datagram{},
	}

	if other {
		count := dgi.ReadUint16()

		DCLock.Lock()
		defer DCLock.Unlock()

		for i := uint16(0); i < count; i++ {
			field := dgi.ReadUint16()
			dcField := dclass.Get_field_by_index(int(field))
			if dcField == dc.SwigcptrDCField(0) {
				s.log.Errorf("Received invalid field index %d", field)
				return
			}

			if !(dcField.Is_required() || dcField.Is_ram()) {
				s.log.Errorf("Recieved NON-RAM field \"%s\" within an OTHER section", dcField.Get_name())
				dgi.SkipDCField(dcField, false)
				continue
			}
			data, ok := dgi.ReadDCField(dcField, true, false)
			if !ok {
				s.log.Errorf("Received invalid update data for field \"%s\"!", dcField.Get_name())
				continue
			}

			obj.fieldUpdates[dcField] = data
		}
	}

	s.loading[do] = &obj
	s.contextToLoading[s.context] = &obj

	// Populate names of required fields to fetch.
	required := make([]string, 0)
	count := dclass.Get_num_inherited_fields()
	for i := 0; i < count; i++ {
		field := dclass.Get_inherited_field(i)
		molecular := field.As_molecular_field().(dc.DCMolecularField)
		if molecular != dc.SwigcptrDCMolecularField(0) {
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

func (s *DatabaseStateServer) initObjectFromDbValues(obj *LoadingObject, dgi *DatagramIterator) {
	do := dgi.ReadDoid()
	if obj.do != do {
		s.log.Warnf("Received GetStoredValues for wrong DOID! %d != %d", obj.do, do)
		s.finalizeLoading(obj)
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

		s.finalizeLoading(obj)
		return
	}

	packedValues := make([][]byte, count)
	hasValue := map[string]bool{}
	for i := uint16(0); i < count; i++ {
		packedValues[i] = dgi.ReadBlob()
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
			continue
		}

		if !(dcField.Is_required() || dcField.Is_ram()) {
			s.log.Errorf("Recieved NON-RAM field \"%s\"", field)
			continue
		}

		if _, ok := obj.fieldUpdates[dcField]; ok {
			// This has already been overridden by the
			// activate_other message earlier, so ignore.
			continue
		}

		if found {
			data := packedValues[i]
			// Validate that the data is correct
			if !ValidateDCRanges(dcField, data) {
				s.log.Errorf("Received invalid update data for field \"%s\"!\n%x", field, data)
				continue
			}
			s.log.Debugf("Got data for field \"%s\": %s", fields[i], FormatFieldData(dcField, data))
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
		if molecular != dc.SwigcptrDCMolecularField(0) {
			continue
		}
		if dcField.Is_required() {
			if data, ok := obj.fieldUpdates[dcField]; ok {
				obj.requiredFields[dcField] = data
				delete(obj.fieldUpdates, dcField)
			} else {
				// Use the default value.
				obj.requiredFields[dcField] = VectorToByte(dcField.Get_default_value())
				s.log.Debugf("Using default value required for field \"%s\" %s", dcField.Get_name(), FormatFieldData(dcField, obj.requiredFields[dcField]))
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
	s.log.Debugf("Replaying %d datagrams to object", len(obj.dgQueue))
	for _, dg := range obj.dgQueue {
		dgi := NewDatagramIterator(&dg)
		dgi.SeekPayload()
		dobj.HandleDatagram(dg, dgi)
	}

	s.finalizeLoading(obj)
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

func (s *DatabaseStateServer) handleGetStoredValues(dgi *DatagramIterator) {
	context := dgi.ReadUint32()
	if obj, ok := s.contextToLoading[context]; ok {
		delete(s.contextToLoading, context)
		s.initObjectFromDbValues(obj, dgi)
		return
	}

	if query, ok := s.contextToFieldQuery[context]; ok {
		delete(s.contextToFieldQuery, context)
		s.finishFieldQuery(query, dgi)
		return
	}

	if query, ok := s.contextToQueryDClass[context]; ok {
		delete(s.contextToQueryDClass, context)
		s.handleDClassQuery(dgi, query)
		return
	}

	if obj, ok := s.contextToQueryAll[context]; ok {
		delete(s.contextToQueryAll, context)
		s.initObjectFromDbValues(obj, dgi)

		if dObj, ok := s.objects[obj.do]; ok {
			s.log.Debugf("handleQueryAll: object id %d successfully initalized, calling handleQueryAll", obj.do)
			dObj.handleQueryAll(obj.queryAllFrom, obj.queryAllContext)
			dObj.annihilate(obj.queryAllFrom, false)
		} else {
			s.log.Errorf("handleQueryAll: Failed to init object id=%d", obj.do)
		}
		return
	}

	s.log.Warnf("Received unknown GetStoredValues context=%d", context)
}

func (s *DatabaseStateServer) handleOneUpdate(dgi *DatagramIterator) {
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

	data, ok := dgi.ReadDCField(field, true, true)

	if !ok || dgi.RemainingSize() > 0 {
		s.log.Errorf("Received invalid update data for field \"%s\"!\n%s", field.Get_name(), dgi)
		return
	}

	s.log.Debugf("Forwarding update for field \"%s\": %s of object id %d to database.\n%s", field.Get_name(), FormatFieldData(field, data), do, dgi)

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(do)
	dg.AddUint16(1) // Field count
	dg.AddString(field.Get_name())
	dg.AddUint16(uint16(len(data)))
	dg.AddData(data)

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

	DCLock.Lock()
	defer DCLock.Unlock()

	fieldUpdates := map[string][]byte{}

	for i := 0; i < int(count); i++ {
		fieldId := dgi.ReadUint16()
		field := core.DC.Get_field_by_index(int(fieldId))
		if field == dc.SwigcptrDCField(0) {
			s.log.Warnf("Update received for unknown field ID=%d", fieldId)
			return
		}

		if !field.Is_db() {
			// Skip the data.
			if !dgi.SkipDCField(field, false) {
				// ..and even that could fail.
				s.log.Errorf("Received invalid update data for field \"%s\"!\n%s", field.Get_name(), dgi)
				return
			}
			continue
		}

		data, ok := dgi.ReadDCField(field, true, false)
		if !ok {
			s.log.Errorf("Received invalid update data for field \"%s\"!\n%s", field.Get_name(), dgi)
			return
		}

		fieldUpdates[field.Get_name()] = data
	}

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(do)
	dg.AddUint16(uint16(len(fieldUpdates)))
	for field, data := range fieldUpdates {
		s.log.Debugf("Forwarding update for field \"%s\" of object id %d to database.", field, do)

		dg.AddString(field)
		dg.AddUint16(uint16(len(data)))
		dg.AddData(data)
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
		fallthrough
	case STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER:
		s.handleGenerate(dgi, msgType == STATESERVER_OBJECT_GENERATE_WITH_REQUIRED_OTHER)
	case STATESERVER_OBJECT_UPDATE_FIELD:
		s.handleOneUpdate(dgi)
	case STATESERVER_OBJECT_UPDATE_FIELD_MULTIPLE:
		s.handleMultipleUpdates(dgi)
	case STATESERVER_OBJECT_QUERY_FIELD:
		fallthrough
	case STATESERVER_OBJECT_QUERY_FIELDS:
		s.handleQueryFields(dgi, sender, msgType == STATESERVER_OBJECT_QUERY_FIELDS)
	case STATESERVER_QUERY_OBJECT_ALL:
		s.handleQueryAll(dgi, sender, Doid_t(receivers[0]))
	case STATESERVER_OBJECT_CREATE_WITH_REQUIRED_CONTEXT:
		fallthrough
	case STATESERVER_OBJECT_CREATE_WITH_REQUIR_OTHER_CONTEXT:
		s.handleActivate(dgi, msgType == STATESERVER_OBJECT_CREATE_WITH_REQUIR_OTHER_CONTEXT)
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
				obj.dgQueue = append(obj.dgQueue, dg)
				s.log.Debugf("Queued message of type=%d", msgType)
			}
		}
		s.log.Debugf("Ignoring message of type=%d", msgType)
	}
}

func (s *DatabaseStateServer) handleQueryFields(dgi *DatagramIterator, sender Channel_t, multiple bool) {
	do := dgi.ReadDoid()
	if _, ok := s.objects[do]; ok {
		s.log.Debugf("Ignoring handleQueryFields of already activated object=%d", do)
		// Let the object instance handle it.
		return
	}
	if obj, ok := s.loading[do]; ok {
		// Wait till the obj has been initalized before handling this message.
		obj.dgQueue = append(obj.dgQueue, *dgi.Dg)
		s.log.Debugf("Queued handleQueryFields for pending object=%d", do)
		return
	}

	var context uint32
	var fields []dc.DCField

	if !multiple {
		fieldId := dgi.ReadUint16()
		context = dgi.ReadUint32()

		field := core.DC.Get_field_by_index(int(fieldId))
		if field == dc.SwigcptrDCField(0) {
			s.log.Errorf("handleQueryFields: Received invalid field index %d", fieldId)
			return
		}
		fields = []dc.DCField{field}
	} else {
		context = dgi.ReadUint32()
		fields = []dc.DCField{}
		for dgi.RemainingSize() >= Blobsize {
			fieldId := dgi.ReadUint16()
			field := core.DC.Get_field_by_index(int(fieldId))
			if field == dc.SwigcptrDCField(0) {
				s.log.Errorf("handleQueryFields: Received invalid field index %d", fieldId)
				return
			}
			fields = append(fields, field)
		}
	}

	name2FieldId := map[string]uint16{}
	for _, field := range fields {
		name2FieldId[field.Get_name()] = uint16(field.Get_number())
	}
	query := &FieldQuery{
		do:      do,
		from:    sender,
		context: context,

		multiple: multiple,
	}

	query.name2FieldId = name2FieldId
	if len(fields) == 1 {
		query.singleFieldId = uint16(fields[0].Get_number())
	}

	s.contextToFieldQuery[s.context] = query

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(s.context)
	dg.AddDoid(do)
	dg.AddUint16(uint16(len(fields)))
	for _, field := range fields {
		dg.AddString(field.Get_name())
	}
	s.RouteDatagram(dg)
	s.context++
}

func (s *DatabaseStateServer) finishFieldQuery(query *FieldQuery, dgi *DatagramIterator) {
	var respMsgType uint16
	if query.multiple {
		respMsgType = STATESERVER_OBJECT_QUERY_FIELDS_RESP
	} else {
		respMsgType = STATESERVER_OBJECT_QUERY_FIELD_RESP
	}

	do := dgi.ReadDoid()
	if do != query.do {
		s.log.Warnf("Got GetStoredValuesResp for id=%d, but was expecting id=%d!", do, query.do)
		dg := NewDatagram()
		dg.AddServerHeader(query.from, Channel_t(query.do), respMsgType)
		dg.AddDoid(query.do)
		if !query.multiple {
			dg.AddUint16(query.singleFieldId)
		}
		dg.AddUint32(query.context)
		dg.AddBool(false) // success
		s.RouteDatagram(dg)
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
			s.log.Errorf("queryFields: Object %d not found in database.", do)
		} else {
			s.log.Errorf("queryFields: GetStoredValues failed for DOID %d", do)
		}

		dg := NewDatagram()
		dg.AddServerHeader(query.from, Channel_t(query.do), respMsgType)
		dg.AddDoid(query.do)
		if !query.multiple {
			dg.AddUint16(query.singleFieldId)
		}
		dg.AddUint32(query.context)
		dg.AddBool(false) // success
		s.RouteDatagram(dg)
		return
	}

	fieldData := map[uint16][]byte{}
	success := true
	for _, field := range fields {
		if fieldId, ok := query.name2FieldId[field]; ok {
			data := dgi.ReadBlob()
			if dgi.ReadBool() { // found
				fieldData[fieldId] = data
			} else {
				s.log.Errorf("queryFields: Data for field \"%s\" not found", field)
				success = false
				break
			}
		} else {
			s.log.Errorf("queryFields: Got unexpected field \"%s\"", field)
			success = false
			break
		}
	}

	dg := NewDatagram()
	dg.AddServerHeader(query.from, Channel_t(query.do), respMsgType)
	dg.AddDoid(query.do)
	if !query.multiple {
		dg.AddUint16(query.singleFieldId)
	}
	dg.AddUint32(query.context)
	dg.AddBool(success)

	if success {
		if !query.multiple {
			dg.AddData(fieldData[query.singleFieldId])
		} else {
			for fieldId, data := range fieldData {
				dg.AddUint16(fieldId)
				dg.AddData(data)
			}
		}
	}
	s.RouteDatagram(dg)
}

func (s *DatabaseStateServer) handleQueryAll(dgi *DatagramIterator, sender Channel_t, do Doid_t) {
	if _, ok := s.objects[do]; ok {
		s.log.Debugf("Ignoring handleQueryAll of already activated object=%d", do)
		// Let the object instance handle it.
		return
	}
	if obj, ok := s.loading[do]; ok {
		// Wait till the obj has been initalized before handling this message.
		obj.dgQueue = append(obj.dgQueue, *dgi.Dg)
		s.log.Debugf("Queued handleQueryAll for pending object=%d", do)
		return
	}

	context := dgi.ReadUint32()
	// First, we need to get the DClass of the stored object, or else we would
	// know what fields we're getting.
	query := &DClassQuery{do, sender, context, *dgi.Dg}
	s.contextToQueryDClass[s.context] = query

	s.log.Debugf("handleQueryAll: Querying DClass name for object id=%d", do)

	dg := NewDatagram()
	dg.AddServerHeader(s.database, Channel_t(do), DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(s.context)
	dg.AddDoid(do)
	dg.AddUint16(1) // count
	dg.AddString("DcObjectType")
	s.RouteDatagram(dg)

	s.context++
}

func (s *DatabaseStateServer) handleDClassQuery(dgi *DatagramIterator, query *DClassQuery) {
	do := dgi.ReadDoid()
	if do != query.do {
		s.log.Errorf("handleDClassQuery: Got GetStoredValuesResp for id=%d, but was expecting id=%d!", do, query.do)
		return
	}

	// Do the checks again just in case our object gets activated while waiting for the
	// database response
	if _, ok := s.objects[do]; ok {
		s.log.Debugf("Ignoring handleQueryAll of already activated object=%d", do)
		// Let the object instance handle it.
		return
	}
	if obj, ok := s.loading[do]; ok {
		// Wait till the obj has been initalized before handling this message.
		obj.dgQueue = append(obj.dgQueue, query.dg)
		s.log.Debugf("Queued handleQueryAll for pending object=%d", do)
		return
	}

	// Skip count and field name
	dgi.Skip(Blobsize)
	dgi.Skip(Dgsize_t(dgi.ReadUint16()))

	code := dgi.ReadUint8()
	if code > 0 {
		if code == 1 {
			s.log.Errorf("Object %d not found in database.", do)
		} else {
			s.log.Errorf("GetStoredValues failed for DOID %d", do)
		}
		return
	}

	// Skip value size.
	dgi.Skip(Blobsize)
	className := dgi.ReadString()
	if !dgi.ReadBool() { // found
		s.log.Errorf("handleQueryAll: Could not find dclass name for object %d.  Does the dclass definition of the object you're looking for has a \"DcObjectType\" parameter?\n\"string DcObjectType db;\"", do)
		return
	}

	s.log.Debugf("handleQueryAll: Found DClass name \"%s\" for object=%d", className, do)
	dclass := core.DC.Get_class_by_name(className)
	if dclass == dc.SwigcptrDCClass(0) {
		s.log.Errorf("handleQueryAll: Retreived unknown class of name \"%s\"!", className)
		return
	}

	// Now thats we've got our name, we can init the object temporary
	// and call handleQueryAll there when finished.
	obj := LoadingObject{
		dbss:   s,
		do:     do,
		parent: INVALID_DOID,
		zone:   INVALID_ZONE,
		dclass: dclass,

		requiredFields: FieldValues{},
		ramFields:      FieldValues{},

		fieldUpdates: FieldValues{},

		context: s.context,
		dgQueue: []Datagram{},

		queryAllFrom:    query.from,
		queryAllContext: query.context,
	}

	s.loading[do] = &obj
	s.contextToQueryAll[s.context] = &obj

	// Populate names of required fields to fetch.
	required := make([]string, 0)
	count := dclass.Get_num_inherited_fields()
	for i := 0; i < count; i++ {
		field := dclass.Get_inherited_field(i)
		molecular := field.As_molecular_field().(dc.DCMolecularField)
		if molecular != dc.SwigcptrDCMolecularField(0) {
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
