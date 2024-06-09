package database

import (
	"fmt"
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"
	"sync"
	"os"
	"os/signal"

	dc "github.com/LittleToonCat/dcparser-go"
	"github.com/apex/log"
)

const (
	CreateObjectOperation uint8 = iota
	GetStoredValuesOperation
	SetStoredValuesOperation
)

type OperationQueueEntry struct {
	operation uint8
	data interface{}
	doId Doid_t
	dclass dc.DCClass
	context uint32
	sender Channel_t
}

type DatabaseBackend interface {
	CreateStoredObject(dclass dc.DCClass, datas map[dc.DCField]dc.Vector_uchar, ctx uint32, sender Channel_t)
	GetStoredValues(doId Doid_t, fields []string, ctx uint32, sender Channel_t)
	SetStoredValues(doId Doid_t, packedValues map[string]dc.Vector_uchar)
}

type Config struct {

	Type      string
	// MONGO BACKEND
	Server    string
	Database  string

	// YAML BACKEND
	Directory string
}

type DatabaseServer struct {
	messagedirector.MDParticipantBase

	config      core.Role
	log         *log.Entry
	control     Channel_t
	min         Doid_t
	max         Doid_t
	objectTypes map[uint16]dc.DCClass
	backend     DatabaseBackend

	queue        []OperationQueueEntry
	queueLock    sync.Mutex
	processQueue chan bool
}

func NewDatabaseServer(config core.Role) *DatabaseServer {
	db := &DatabaseServer{
		config: config,
		control: Channel_t(config.Control),
		queue: []OperationQueueEntry{},
		processQueue: make(chan bool),
		min: Doid_t(config.Generate.Min),
		max: Doid_t(config.Generate.Max),
		objectTypes: make(map[uint16]dc.DCClass),
		log: log.WithFields(log.Fields{
			"name":    fmt.Sprintf("DatabaseServer (%d)", config.Control),
			"modName": "DatabaseServer",
			"id":      fmt.Sprintf("%d", config.Control),
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

	if ok, backend, err := db.createBackend(config); ok {
		db.backend = backend
	} else {
		db.log.Fatal(err.Error())
	}

	db.Init(db)
	db.SetName(fmt.Sprintf("DatabaseServer (%d)", config.Control))

	db.SubscribeChannel(Channel_t(db.control))
	db.SubscribeChannel(BCHAN_DBSERVERS)

	go db.queueLoop()

	return db
}

func (d *DatabaseServer) createBackend(config core.Role) (bool, DatabaseBackend, error) {
	switch config.Backend.Type {
	case "mongodb":
		return NewMongoBackend(d, config.Backend)
	case "yaml":
		return NewYAMLBackend(d, config.Backend)
	default:
		return false, nil, fmt.Errorf("unknown backend type: %s", config.Backend.Type)
	}
}

func (d *DatabaseServer) getOperationFromQueue() OperationQueueEntry {
	d.queueLock.Lock()
	defer d.queueLock.Unlock()

	op := d.queue[0]
	d.queue[0] = OperationQueueEntry{}
	d.queue = d.queue[1:]
	return op
}

func (d *DatabaseServer) queueLoop() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	for {
		select {
		case <-d.processQueue:
			for len(d.queue) > 0 {
				op := d.getOperationFromQueue()
				switch op.operation {
				case CreateObjectOperation:
					d.backend.CreateStoredObject(op.dclass, op.data.(map[dc.DCField]dc.Vector_uchar), op.context, op.sender)
				case GetStoredValuesOperation:
					d.backend.GetStoredValues(op.doId, op.data.([]string), op.context, op.sender)
				case SetStoredValuesOperation:
					d.backend.SetStoredValues(op.doId, op.data.(map[string]dc.Vector_uchar))
				}
			}
		case <-signalCh:
			return
		case <-core.StopChan:
			return
		}
	}
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

	d.queueLock.Lock()
	op := OperationQueueEntry{operation: CreateObjectOperation,
		dclass: dclass, data: datas, context: context, sender: sender}
	d.queue = append(d.queue, op)
	d.queueLock.Unlock()

	select {
	case d.processQueue <- true:
	default:
	}
}

func (d *DatabaseServer) HandleGetStoredValues(dgi *DatagramIterator, sender Channel_t) {
	context := dgi.ReadUint32()
	doId := dgi.ReadDoid()
	count := dgi.ReadUint16()

	requestedFields := make([]string, count)
	for i := uint16(0); i < count; i++ {
		requestedFields[i] = dgi.ReadString()
	}

	d.queueLock.Lock()
	op := OperationQueueEntry{operation: GetStoredValuesOperation,
		doId: doId, data: requestedFields, context: context, sender: sender}
	d.queue = append(d.queue, op)
	d.queueLock.Unlock()

	select {
	case d.processQueue <- true:
	default:
	}
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

	d.queueLock.Lock()
	op := OperationQueueEntry{operation: SetStoredValuesOperation,
		doId: doId, data: packedValues}
	d.queue = append(d.queue, op)
	d.queueLock.Unlock()

	select {
	case d.processQueue <- true:
	default:
	}
}
