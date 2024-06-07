package luarole

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"
	"sync"

	dc "github.com/LittleToonCat/dcparser-go"
	"github.com/apex/log"
	gluahttp "github.com/cjoudrey/gluahttp"
	gluacrypto "github.com/tengattack/gluacrypto"
	libs "github.com/vadv/gopher-lua-libs"
	lua "github.com/yuin/gopher-lua"
)

type LuaQueueEntry struct {
	fn     lua.LValue
	sender Channel_t
	args   []lua.LValue
}

type LuaRole struct {
	messagedirector.MDParticipantBase
	sync.Mutex

	config core.Role
	log    *log.Entry

	// We store the sender internally because Lua has no native
	// uint64 support, and passing it as a LNumber (which is
	// a float64 type) may cause some problems.
	sender Channel_t

	context          uint32
	createContextMap map[uint32]func(doId Doid_t)
	getContextMap    map[uint32]func(doId Doid_t, dgi *DatagramIterator)
	queryContextMap  map[uint32]func(dgi *DatagramIterator)

	L            *lua.LState
	LQueue       []LuaQueueEntry
	processQueue chan bool
}

func NewLuaRole(config core.Role) *LuaRole {
	var name string
	if len(config.Name) > 0 {
		name = config.Name
	} else {
		name = "Lua"
	}

	role := &LuaRole{
		config: config,
		log: log.WithFields(log.Fields{
			"name":    name,
			"modName": name,
		}),
		createContextMap: map[uint32]func(doId Doid_t){},
		getContextMap:    map[uint32]func(doId Doid_t, dgi *DatagramIterator){},
		queryContextMap:  map[uint32]func(dgi *DatagramIterator){},
		L:                lua.NewState(),
		LQueue:           []LuaQueueEntry{},
		processQueue:     make(chan bool),
	}

	role.Init(role)
	role.SetName(name)

	libs.Preload(role.L)
	// Replace gopher-lua-libs's crypto module with
	// gluacrypto since it has more methods.
	gluacrypto.Preload(role.L)
	// Used for web requests within Lua
	role.L.PreloadModule("http", gluahttp.NewHttpModule(&http.Client{}).Loader)
	RegisterLuaUtilTypes(role.L)
	core.RegisterLuaDCTypes(role.L)
	RegisterLuaParticipantType(role.L)

	// Set globals
	role.L.SetGlobal("dcFile", core.NewLuaDCFile(role.L, core.DC))

	role.log.Infof("Running Lua script: %s", role.config.Lua_File)
	if err := role.L.DoFile(role.config.Lua_File); err != nil {
		role.log.Fatal(err.Error())
		return nil
	}

	// Santity check to make sure certian global functions exists:
	if _, ok := role.L.GetGlobal("handleDatagram").(*lua.LFunction); !ok {
		role.log.Fatal("Missing \"handleDatagram\" function in Lua script.")
		return nil
	}

	// Call the init function if there's any:
	if initFunction, ok := role.L.GetGlobal("init").(*lua.LFunction); ok {
		err := role.L.CallByParam(lua.P{
			Fn:      initFunction,
			NRet:    0,
			Protect: true,
		}, NewLuaParticipant(role.L, role))
		if err != nil {
			role.log.Fatal(err.Error())
			return nil
		}
	}

	go role.queueLoop()
	return role
}

func (l *LuaRole) getEntryFromQueue() LuaQueueEntry {
	l.Lock()
	defer l.Unlock()

	op := l.LQueue[0]
	l.LQueue = l.LQueue[1:]
	return op
}

func (l *LuaRole) queueLoop() {
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)

	for {
		select {
		case <-l.processQueue:
			for len(l.LQueue) > 0 {
				entry := l.getEntryFromQueue()
				// Store last sender.
				l.sender = entry.sender
				err := l.L.CallByParam(lua.P{
					Fn:      entry.fn,
					NRet:    0,
					Protect: true,
				}, entry.args...)
				if err != nil {
					l.log.Errorf("Lua error:\n%s", err.Error())
				}
			}
		case <-signalCh:
			return
		case <-core.StopChan:
			return
		}
	}
}

func (l *LuaRole) CallLuaFunction(fn lua.LValue, sender Channel_t, args ...lua.LValue) {
	// Queue the call
	l.Lock()
	entry := LuaQueueEntry{fn, sender, args}
	l.LQueue = append(l.LQueue, entry)
	l.Unlock()

	select {
	case l.processQueue <- true:
	default:
	}
}

func (l *LuaRole) HandleDatagram(dg Datagram, dgi *DatagramIterator) {
	sender := dgi.ReadChannel()
	msgType := dgi.ReadUint16()

	switch msgType {
	case STATESERVER_OBJECT_QUERY_FIELDS_RESP:
		l.handleQueryFieldsResp(dgi)
	case DBSERVER_CREATE_STORED_OBJECT_RESP:
		context, code, doId := dgi.ReadUint32(), dgi.ReadUint8(), dgi.ReadDoid()
		l.handleCreateDatabaseResp(context, code, doId)
	case DBSERVER_GET_STORED_VALUES_RESP:
		l.handleGetStoredValuesResp(dgi)
	default:
		// Let Lua handle it.
		l.CallLuaFunction(l.L.GetGlobal("handleDatagram"), sender,
			// Arguments:
			NewLuaParticipant(l.L, l),
			lua.LNumber(msgType),
			NewLuaDatagramIteratorFromExisting(l.L, dgi))
	}
}

func (c *LuaRole) createDatabaseObject(dbChannel Channel_t, objectType uint16, packedValues map[string]dc.Vector_uchar, from Channel_t, callback func(doId Doid_t)) {
	c.createContextMap[c.context] = callback

	dg := NewDatagram()
	dg.AddServerHeader(dbChannel, from, DBSERVER_CREATE_STORED_OBJECT)
	dg.AddUint32(c.context)
	dg.AddString("") // Unknown
	dg.AddUint16(objectType)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector_uchar(value)
	}
	c.RouteDatagram(dg)
	c.context++
}

func (c *LuaRole) handleCreateDatabaseResp(context uint32, code uint8, doId Doid_t) {
	callback, ok := c.createContextMap[context]
	if !ok {
		c.log.Warnf("Got CreateDatabaseRsp with missing context %d", context)
		return
	}

	if code > 0 {
		c.log.Warnf("CreateDatabaseResp returned an error!")
	}

	callback(doId)
	delete(c.createContextMap, context)
}

func (l *LuaRole) getDatabaseValues(dbChannel Channel_t, doId Doid_t, fields []string, from Channel_t, callback func(doId Doid_t, dgi *DatagramIterator)) {
	l.getContextMap[l.context] = callback

	dg := NewDatagram()
	dg.AddServerHeader(dbChannel, from, DBSERVER_GET_STORED_VALUES)
	dg.AddUint32(l.context)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fields)))
	for _, name := range fields {
		dg.AddString(name)
	}
	l.RouteDatagram(dg)
	l.context++
}

func (l *LuaRole) handleGetStoredValuesResp(dgi *DatagramIterator) {
	context := dgi.ReadUint32()
	doId := dgi.ReadDoid()

	callback, ok := l.getContextMap[context]
	if !ok {
		l.log.Warnf("Got GetStoredResp with missing context %d", context)
		return
	}

	callback(doId, dgi)
	delete(l.getContextMap, context)
}

func (l *LuaRole) handleQueryFieldsResp(dgi *DatagramIterator) {
	dgi.ReadDoid() // doId, unused

	context := dgi.ReadUint32()

	callback, ok := l.queryContextMap[context]
	if !ok {
		l.log.Warnf("Got QueryFieldsResp with missing context %d", context)
		return
	}

	callback(dgi)
	delete(l.queryContextMap, context)
}

func (l *LuaRole) setDatabaseValues(doId Doid_t, dbChannel Channel_t, packedValues map[string]dc.Vector_uchar) {
	dg := NewDatagram()
	dg.AddServerHeader(dbChannel, 0, DBSERVER_SET_STORED_VALUES)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(packedValues)))

	for name, value := range packedValues {
		dg.AddString(name)
		dg.AddUint16(uint16(value.Size()))
		dg.AddVector(value)
		dc.DeleteVector_uchar(value)
	}

	l.RouteDatagram(dg)
}

func (l *LuaRole) handleUpdateField(dgi *DatagramIterator, className string) {
	dclass := core.DC.Get_class_by_name(className)
	if dclass == dc.SwigcptrDCClass(0) {
		l.log.Errorf("handleUpdateField: Class \"%s\" does not exist!", className)
		return
	}
	fieldId := dgi.ReadUint16()
	dcField := dclass.Get_field_by_index(int(fieldId))
	if dcField == dc.SwigcptrDCField(0) {
		l.log.Errorf("handleUpdateField: Field number %d does not exist in class \"%s\"!", fieldId, className)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()
	packedData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector_uchar(packedData)
	if !dcField.Validate_ranges(packedData) {
		l.log.Errorf("Received invalid update data for field \"%s\"!\n%s\n%s", dcField.Get_name(), DumpVector(packedData), dgi)
		return
	}

	lFunc := l.L.GetGlobal(fmt.Sprintf("handle%s_%s", dclass.Get_name(), dcField.Get_name()))
	if lFunc.Type() != lua.LTFunction {
		l.log.Warnf("Function \"handle%s_%s\" does not exist in Lua file!", className, dcField.Get_name())
		return
	}
	// Call the Lua function
	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	unpacker.Set_unpack_data(packedData)
	unpacker.Begin_unpack(dcField)
	lValue := core.UnpackDataToLuaValue(unpacker, l.L)
	if !unpacker.End_unpack() {
		l.log.Warnf("End_unpack returned false on handleUpdateField somehow...\n%s", DumpUnpacker(unpacker))
		return
	}
	l.CallLuaFunction(lFunc, l.sender, NewLuaParticipant(l.L, l), lua.LNumber(fieldId), lValue)
}

func (l *LuaRole) sendUpdateToChannel(channel Channel_t, fromDoId Doid_t, className string, fieldName string, value lua.LValue) {
	cls := core.DC.Get_class_by_name(className)
	if cls == dc.SwigcptrDCClass(0) {
		l.log.Warnf("sendUpdateToChannel: Class name \"%s\" not found!", className)
		return
	}

	field := cls.Get_field_by_name(fieldName)
	if field == dc.SwigcptrDCField(0) {
		l.log.Warnf("sendUpdateToChannel: Class \"%s\" does not have field \"%s\"!", className, fieldName)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packer.Begin_pack(field)
	core.PackLuaValue(packer, value)
	if !packer.End_pack() {
		l.log.Warnf("sendUpdateToChannel: Packing of \"%s\" failed!", fieldName)
		return
	}

	packedData := packer.Get_bytes()
	defer dc.DeleteVector_uchar(packedData)

	dg := NewDatagram()
	dg.AddServerHeader(channel, Channel_t(fromDoId), STATESERVER_OBJECT_UPDATE_FIELD)
	dg.AddDoid(fromDoId)
	dg.AddUint16(uint16(field.Get_number()))
	dg.AddVector(packedData)
	l.RouteDatagram(dg)
}
