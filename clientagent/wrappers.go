package clientagent

import (
	"errors"
	"fmt"
	"otpgo/core"
	. "otpgo/util"

	dc "github.com/LittleToonCat/dcparser-go"
	"github.com/yuin/gopher-lua"
)

// Client wrappers for Lua

const luaClientType = "client"

func RegisterClientType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaClientType)
	L.SetGlobal(luaClientType, mt)
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), ClientMethods))
}

func NewLuaClient(L *lua.LState, c *Client) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = c
	L.SetMetatable(ud, L.GetTypeMetatable(luaClientType))
	return ud
}

func CheckClient(L *lua.LState, n int) *Client {
	ud := L.CheckUserData(n)
	if client, ok := ud.Value.(*Client); ok {
		return client
	}
	L.ArgError(n, "Client expected")
	return nil
}

var ClientMethods = map[string]lua.LGFunction{
	"addSessionObject": LuaAddSessionObject,
	"authenticated": LuaGetSetAuthenticated,
	"createDatabaseObject": LuaCreateDatabaseObject,
	"getDatabaseValues": LuaGetDatabaseValues,
	"setDatabaseValues": LuaSetDatabaseValues,
	"handleAddInterest": LuaHandleAddInterest,
	"handleDisconnect": LuaHandleDisconnect,
	"handleHeartbeat": LuaHandleHeartbeat,
	"handleRemoveInterest": LuaHandleRemoveInterest,
	"handleUpdateField": LuaHandleUpdateField,
	"objectSetOwner": LuaObjectSetOwner,
	"sendActivateObject": LuaSendActivateObject,
	"sendDatagram": LuaSendDatagram,
	"sendDisconnect": LuaSendDisconnect,
	"setLocation": LuaSetLocation,
	"subscribeChannel": LuaSubscribeChannel,
	"subscribePuppetChannel": LuaSubscribePuppetChannel,
	"setChannel": LuaSetChannel,
	"unsubscribePuppetChannel": LuaUnsubscribePuppetChannel,
	"userTable": LuaGetSetUserTable,
}

func LuaGetSetAuthenticated(L *lua.LState) int {
	client := CheckClient(L, 1)
	if L.GetTop() == 2 {
		state := L.CheckBool(2)
		client.authenticated = state
	} else {
		L.Push(lua.LBool(client.authenticated))
	}
	return 1
}

func LuaCreateDatabaseObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	clsName := L.CheckString(2)
	fields := L.CheckTable(3)
	objectType := L.CheckInt(4)
	callback := L.CheckFunction(5)

	cls := core.DC.Get_class_by_name(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(2, "Class not found.")
		return 0
	}

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedFields := map[string]dc.Vector_uchar{}
	// TODO: string dictionary sanity check
	fields.ForEach(func(l1, data lua.LValue) {
		name := string(l1.(lua.LString))
		field := cls.Get_field_by_name(name)
		if field == dc.SwigcptrDCField(0) {
			L.ArgError(3, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", name, clsName))
			return
		}
		packer.Begin_pack(field)
		core.PackLuaValue(packer, data)
		if !packer.End_pack() {
			L.ArgError(3, "Pack failed!")
			return
		}

		packedFields[name] = packer.Get_bytes()
		packer.Clear_data()
	})

	callbackFunc := func(doId Doid_t) {
		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId))
	}

	client.createDatabaseObject(uint16(objectType), packedFields, callbackFunc)

	return 1
}

func LuaSendDisconnect(L *lua.LState) int {
	client := CheckClient(L, 1)
	reason := L.CheckInt(2)
	error := L.CheckString(3)
	security := L.CheckBool(4)

	go client.sendDisconnect(uint16(reason), error, security)
	return 1
}

func LuaHandleHeartbeat(L *lua.LState) int {
	client := CheckClient(L, 1)
	client.handleHeartbeat()
	return 1
}

func LuaHandleDisconnect(L *lua.LState) int {
	client := CheckClient(L, 1)
	client.cleanDisconnect = true
	client.Terminate(errors.New(""))
	return 1
}

func LuaSendDatagram(L *lua.LState) int {
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	go client.client.SendDatagram(*dg)
	return 1
}

func LuaGetDatabaseValues(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fieldsTable := L.CheckTable(4)
	callback := L.CheckFunction(5)

	cls := core.DC.Get_class_by_name(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	fields := make([]string, 0)
	fieldsTable.ForEach(func(_, l2 lua.LValue) {
		fieldName := l2.(lua.LString)
		fields = append(fields, string(fieldName))
	})

	callbackFunc := func(dbDoId Doid_t, dgi *DatagramIterator)  {
		if doId != dbDoId {
			client.log.Warnf("Got GetStoredValues for wrong ID! Got: %d.  Expecting: %d", dbDoId, doId)
			client.ca.CallLuaFunction(callback, client, lua.LFalse, lua.LNil)
			return
		}

		count := dgi.ReadUint16()
		fields := make([]string, count)
		for i := uint16(0); i < count; i++ {
			fields[i] = dgi.ReadString()
		}

		code := dgi.ReadUint8()
		if code > 0 {
			client.log.Warnf("GetStoredValues returned error code %d", code)
			client.ca.CallLuaFunction(callback, client, lua.LFalse, lua.LNil)
			return
		}

		packedValues := make([]dc.Vector_uchar, count)
		hasValue := map[string]bool{}
		for i := uint16(0); i < count; i++ {
			packedValues[i] = dgi.ReadVector()
			hasValue[fields[i]] = dgi.ReadBool()
			if !hasValue[fields[i]] {
				client.log.Debugf("GetStoredValues: Data for field \"%s\" not found", fields[i])
			}
		}

		fieldTable := L.NewTable()
		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		for i := uint16(0); i < count; i++ {
			field := fields[i]
			found := hasValue[field]

			dcField := cls.Get_field_by_name(field)
			if dcField == dc.SwigcptrDCField(0) {
				client.log.Warnf("GetStoredValues: Field \"%s\" does not exist for class \"%s\"", field, clsName)
				if found {
					dc.DeleteVector_uchar(packedValues[i])
				}
				continue
			}

			if found {
				data := packedValues[i]
				// Validate that the data is correct
				if !dcField.Validate_ranges(data) {
					client.log.Errorf("GetStoredValues: Received invalid data for field \"%s\"!", field)
					dc.DeleteVector_uchar(data)
					continue
				}

				unpacker.Set_unpack_data(data)
				unpacker.Begin_unpack(dcField)
				fieldTable.RawSetString(fields[i], core.UnpackDataToLuaValue(unpacker, L))
				unpacker.End_unpack()

				dc.DeleteVector_uchar(data)
			}
		}
		client.ca.CallLuaFunction(callback, client, lua.LTrue, fieldTable)
	}

	client.getDatabaseValues(doId, fields, callbackFunc)
	return 1
}

func LuaSetDatabaseValues(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fields := L.CheckTable(4)

	cls := core.DC.Get_class_by_name(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(2, "Class not found.")
		return 0
	}

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedFields := map[string]dc.Vector_uchar{}
	// TODO: string dictionary sanity check
	fields.ForEach(func(l1, data lua.LValue) {
		name := string(l1.(lua.LString))
		field := cls.Get_field_by_name(name)
		if field == dc.SwigcptrDCField(0) {
			L.ArgError(3, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", name, clsName))
			return
		}
		packer.Begin_pack(field)
		core.PackLuaValue(packer, data)
		if !packer.End_pack() {
			L.ArgError(3, "Pack failed!")
			return
		}

		packedFields[name] = packer.Get_bytes()
		packer.Clear_data()
	})

	client.setDatabaseValues(doId, packedFields)

	return 1
}

func LuaGetSetUserTable(L *lua.LState) int {
	client := CheckClient(L, 1)
	if L.GetTop() == 2 {
		table := L.CheckTable(2)
		client.userTable = table;
	} else {
		if client.userTable == nil {
			client.userTable = L.NewTable()
		}
		L.Push(client.userTable)
	}
	return 1
}

func LuaHandleAddInterest(L *lua.LState) int {
	client := CheckClient(L, 1)

	var handle uint16
	var context uint32
	var parent Doid_t
	zones := []Zone_t{}

	if L.GetTop() == 2 {
		// client:handleAddInterest(dgi)
		dgi := CheckDatagramIterator(L, 2)
		handle = dgi.ReadUint16()
		context = dgi.ReadUint32()
		parent = dgi.ReadDoid()
		for dgi.RemainingSize() > 0 {
			zones = append(zones, dgi.ReadZone())
		}
	} else {
		// client:handleAddInterest(handle, context, parent, {zone...})
		handle = uint16(L.CheckInt(2))
		context = uint32(L.CheckInt(3))
		parent = Doid_t(L.CheckInt(4))
		zonesTable := L.CheckTable(5)

		zonesTable.ForEach(func(_, l2 lua.LValue) {
			zone := l2.(lua.LNumber)
			zones = append(zones, Zone_t(zone))
		})
	}

	i := client.buildInterest(handle, parent, zones)
	client.addInterest(i, context, 0)

	return 1
}

func LuaHandleRemoveInterest(L *lua.LState) int {
	client := CheckClient(L, 1)

	var handle uint16
	var context uint32

	if L.GetTop() == 2 {
		// client:handleRemoveInterest(dgi)
		dgi := CheckDatagramIterator(L, 2)
		handle = dgi.ReadUint16()
		context = uint32(0)
		if dgi.RemainingSize() == Dgsize {
			context = dgi.ReadUint32()
		}
	} else {
		// client:handleRemoveInterest(handle, context)
		handle = uint16(L.CheckInt(2))
		context = uint32(L.CheckInt(3))
	}

	if i, ok := client.interests[handle]; ok {
		client.removeInterest(i, context)
	} else {
		client.sendDisconnect(CLIENT_DISCONNECT_GENERIC, fmt.Sprintf("Attempted to remove non-existant interest: %d", handle), true)
	}

	return 1
}

func LuaSubscribeChannel(L *lua.LState) int {
	client := CheckClient(L, 1)
	channel := Channel_t(L.CheckInt(2))
	client.SubscribeChannel(channel)
	return 1
}

func LuaSetChannel(L *lua.LState) int {
	client := CheckClient(L, 1)

	var channel Channel_t
	if L.GetTop() == 2 {
		// client:setChannel(channel)
		channel = Channel_t(L.CheckInt64(2))
	} else {
		// client:setChannel(accountId, avatarId)
		account := L.CheckInt(2)
		avatar := L.CheckInt(3)
		channel = Channel_t(account) << 32 | Channel_t(avatar)
	}
	client.SetChannel(channel)
	return 1
}

func LuaSubscribePuppetChannel(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Channel_t(L.CheckInt(2))
	puppetType := Channel_t(L.CheckInt(3))

	client.SubscribeChannel(do + puppetType << 32)
	return 1
}

func LuaUnsubscribePuppetChannel(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Channel_t(L.CheckInt(2))
	puppetType := Channel_t(L.CheckInt(3))

	client.UnsubscribeChannel(do + puppetType << 32)
	return 1
}

func LuaHandleUpdateField(L *lua.LState) int {
	client := CheckClient(L, 1)
	dgi := CheckDatagramIterator(L, 2)

	do, field := dgi.ReadDoid(), dgi.ReadUint16()
	client.handleClientUpdateField(do, field, dgi)

	return 1
}

func LuaSendActivateObject(L * lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))
	className := L.CheckString(3)

	dclass := core.DC.Get_class_by_name(className)
	if dclass == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class does not exist.")
		return 0
	}

	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(do), client.channel, DBSS_OBJECT_ACTIVATE_WITH_DEFAULTS)
	dg.AddDoid(do)
	dg.AddLocation(0, 0)
	dg.AddUint16(uint16(dclass.Get_number()))
	client.RouteDatagram(dg)
	return 1
}

func LuaObjectSetOwner(L * lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))
	all := L.CheckBool(3)

	msgType := uint16(STATESERVER_OBJECT_SET_OWNER_RECV)
	if all {
		msgType = STATESERVER_OBJECT_SET_OWNER_RECV_WITH_ALL
	}

	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(do), client.channel, msgType)
	dg.AddChannel(client.channel)
	client.RouteDatagram(dg)
	return 1
}

func LuaAddSessionObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))

	for _, d := range client.sessionObjects {
		if d == do {
			client.log.Warnf("Received add sesion object with existing ID=%d", do)
		}
	}

	client.log.Debugf("Added session object with ID %d", do)
	client.sessionObjects = append(client.sessionObjects, do)
	return 1
}

func LuaSetLocation(L *lua.LState) int {
	client := CheckClient(L, 1)
	dgi := CheckDatagramIterator(L, 2)

	do := dgi.ReadDoid()
	parent := dgi.ReadDoid()
	zone := dgi.ReadZone()

	if obj, ok := client.ownedObjects[do]; ok {
		obj.parent = parent
		obj.zone = zone

		dg := NewDatagram()
		dg.AddServerHeader(Channel_t(do), client.channel, STATESERVER_OBJECT_SET_ZONE)
		dg.AddDoid(parent)
		dg.AddZone(zone)
		client.RouteDatagram(dg)
	} else {
		client.sendDisconnect(CLIENT_DISCONNECT_FORBIDDEN_RELOCATE, fmt.Sprintf("Attempted to move un-owned object %d", do), true)
	}
	return 1
}
