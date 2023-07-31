package database

import (
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"
	"fmt"
	"github.com/apex/log"
	dc "github.com/LittleToonCat/dcparser-go"
)

type DatabaseServer struct {
	messagedirector.MDParticipantBase

	config      core.Role
	log         *log.Entry
	control     Channel_t
	min         Doid_t
	max         Doid_t
	objectTypes map[uint16]dc.DCClass

	// TODO: Support for other backends (YAML, SQL)
	backend     *MongoBackend
}

func NewDatabaseServer(config core.Role) *DatabaseServer {
	db := &DatabaseServer{
		config: config,
		control: Channel_t(config.Control),
		min: Doid_t(config.Generate.Min),
		max: Doid_t(config.Generate.Max),
		objectTypes: make(map[uint16]dc.DCClass),
		log: log.WithFields(log.Fields{
			"name": fmt.Sprintf("DatabaseServer (%d)", config.Control),
		}),
	}

	// Populate object types
	for _, obj := range config.Objects {
		dclass := core.DC.Get_class_by_name(obj.Class)
		if dclass == dc.SwigcptrDCClass(0) {
			db.log.Fatalf("For object type %d, \"%s\" does not exist!", obj.ID, obj.Class)
		}
		db.objectTypes[uint16(obj.ID)] = dclass
	}

	if ok, backend, err := NewMongoBackend(db, config.Backend); ok {
		db.backend = backend
		} else {
			db.log.Fatal(err.Error())
		}

	db.Init(db)

	db.SubscribeChannel(Channel_t(db.control))
	db.SubscribeChannel(BCHAN_DBSERVERS)

	return db
}

func (d *DatabaseServer) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
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
	case DBSERVER_CREATE_STORED_OBJECT:
		d.HandleCreateObject(dgi, sender)
	case DBSERVER_GET_STORED_VALUES:
		d.HandleGetStoredValues(dgi, sender)
	case DBSERVER_SET_STORED_VALUES:
		d.handleSetStoredValues(dgi, sender)
	default:
		d.log.Warnf("Received unknown msgtype=%d", msgType)
	}
}

func (d *DatabaseServer) HandleCreateObject(dgi *DatagramIterator, sender Channel_t) {
	context := dgi.ReadUint32()
	dgi.ReadString() // Unknown
	objectType := dgi.ReadUint16()

	dclass, ok := d.objectTypes[objectType]
	if !ok {
		d.log.Errorf("CreateObject: Class of type %d does not exist!", objectType)
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, d.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(context)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		d.RouteDatagram(dg)
		return
	}

	count := dgi.ReadUint16()
	datas := map[dc.DCField]dc.Vector_uchar{}

	for i := uint16(0); i < count; i++ {
		name := dgi.ReadString()
		blob := dgi.ReadVector()

		field := dclass.Get_field_by_name(name)
		if field == dc.SwigcptrDCField(0) {
			log.Errorf("Field \"%s\" does not exist for class \"%s\"!", name, dclass.Get_name())
		}

		datas[field] = blob
	}

	go d.backend.CreateStoredObject(dclass, datas, context, sender)
}

func (d *DatabaseServer) HandleGetStoredValues(dgi *DatagramIterator, sender Channel_t) {
	context := dgi.ReadUint32()
	doId := dgi.ReadDoid()
	count := dgi.ReadUint16()

	requestedFields := make([]string, count)
	for i := uint16(0); i < count; i++ {
		requestedFields[i] = dgi.ReadString()
	}

	go d.backend.GetStoredValues(doId, requestedFields, context, sender)
}

func (d *DatabaseServer) handleSetStoredValues(dgi *DatagramIterator, sender Channel_t) {
	doId := dgi.ReadDoid()
	count := dgi.ReadUint16()

	packedValues := map[string]dc.Vector_uchar{}

	for i := uint16(0); i < count; i++ {
		field := dgi.ReadString()
		value := dgi.ReadVector()
		packedValues[field] = value
	}

	go d.backend.SetStoredValues(doId, packedValues)
}
