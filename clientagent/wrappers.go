package clientagent

import (
	"errors"
	"fmt"
	"otpgo/core"
	"otpgo/eventlogger"
	. "otpgo/util"
	"slices"
	"strconv"

	"otpgo/dc"

	lua "github.com/yuin/gopher-lua"
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
	"addServerHeader":              LuaClientAddServerHeader,
	"addServerHeaderWithAvatarId":  LuaAddServerHeaderWithAvatarId,
	"addServerHeaderWithAccountId": LuaAddServerHeaderWithAccountId,
	"addSessionObject":             LuaAddSessionObject,
	"addPostRemove":                LuaAddPostRemove,
	"authenticated":                LuaGetSetAuthenticated,
	"clearPostRemoves":             LuaClearPostRemoves,
	"createDatabaseObject":         LuaCreateDatabaseObject,
	"declareObject":                LuaDeclareObject,
	"debug":                        LuaDebug,
	"error":                        LuaError,
	"getAllRequiredFromDatabase":   LuaGetAllRequiredFromDatabase,
	"getDatabaseValues":            LuaGetDatabaseValues,
	"setDatabaseValues":            LuaSetDatabaseValues,
	"handleAddInterest":            LuaHandleAddInterest,
	"handleDisconnect":             LuaHandleDisconnect,
	"handleHeartbeat":              LuaHandleHeartbeat,
	"handleRemoveInterest":         LuaHandleRemoveInterest,
	"handleUpdateField":            LuaHandleUpdateField,
	"info":                         LuaInfo,
	"objectSetOwner":               LuaObjectSetOwner,
	"packFieldToDatagram":          LuaPackFieldToDatagram,
	"queryAllRequiredFields":       LuaQueryAllRequiredFields,
	"queryObjectFields":            LuaQueryObjectFields,
	"removeSessionObject":          LuaRemoveSessionObject,
	"routeDatagram":                LuaRouteDatagram,
	"sendActivateObject":           LuaSendActivateObject,
	"sendDatagram":                 LuaSendDatagram,
	"sendDisconnect":               LuaSendDisconnect,
	"setLocation":                  LuaSetLocation,
	"subscribeChannel":             LuaSubscribeChannel,
	"subscribePuppetChannel":       LuaSubscribePuppetChannel,
	"setChannel":                   LuaSetChannel,
	"undeclareObject":              LuaUndeclareObject,
	"undeclareAllObjects":          LuaUndeclareAllObjects,
	"unsubscribePuppetChannel":     LuaUnsubscribePuppetChannel,
	"userTable":                    LuaGetSetUserTable,
	"warn":                         LuaWarn,
	"writeServerEvent":             LuaWriteServerEvent,
}

func LuaInfo(L *lua.LState) int {
	client := CheckClient(L, 1)
	msg := L.CheckString(2)

	client.log.Info(msg)
	return 1
}

func LuaWarn(L *lua.LState) int {
	client := CheckClient(L, 1)
	msg := L.CheckString(2)

	client.log.Warn(msg)
	return 1
}

func LuaError(L *lua.LState) int {
	client := CheckClient(L, 1)
	msg := L.CheckString(2)

	client.log.Error(msg)
	return 1
}

func LuaDebug(L *lua.LState) int {
	client := CheckClient(L, 1)
	msg := L.CheckString(2)

	client.log.Debug(msg)
	return 1
}

func LuaClientAddServerHeader(L *lua.LState) int {
	// This exists because of Lua cannot add
	// the server header on its own, espically since
	// our channel can be higher than what Lua
	// is allowed.
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	to := Channel_t(L.CheckNumber(3))
	msgType := uint16(L.CheckNumber(4))

	dg.AddServerHeader(to, client.channel, msgType)
	return 1
}

func LuaAddServerHeaderWithAvatarId(L *lua.LState) int {
	// This exists because of Lua cannot add
	// the avatar puppet channel on its own.
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	avatarId := (L.CheckNumber(3))
	msgType := uint16(L.CheckNumber(4))

	dg.AddServerHeader(Channel_t(avatarId+(1<<32)), client.channel, msgType)
	return 1
}

func LuaAddServerHeaderWithAccountId(L *lua.LState) int {
	// This exists because of Lua cannot add
	// the account puppet channel on its own.
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	accountId := (L.CheckNumber(3))
	msgType := uint16(L.CheckNumber(4))

	dg.AddServerHeader(Channel_t(accountId+(3<<32)), client.channel, msgType)
	return 1
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

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(2, "Class not found.")
		return 0
	}

	DCLock.Lock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedFields := map[string]dc.Vector{}
	// TODO: string dictionary sanity check
	fields.ForEach(func(l1, data lua.LValue) {
		name := string(l1.(lua.LString))
		field := cls.GetFieldByName(name)
		if field == dc.SwigcptrDCField(0) {
			L.ArgError(3, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", name, clsName))
			return
		}
		packer.BeginPack(field)
		core.PackLuaValue(packer, data)
		if !packer.EndPack() {
			L.ArgError(3, "Pack failed!")
			return
		}

		packedFields[name] = packer.GetBytes()
		packer.ClearData()
	})

	DCLock.Unlock()
	callbackFunc := func(doId Doid_t) {
		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId))
	}

	client.createDatabaseObject(uint16(objectType), packedFields, callbackFunc)

	return 1
}

func LuaPackFieldToDatagram(L *lua.LState) int {
	dg := CheckDatagram(L, 2)
	clsName := L.CheckString(3)
	fieldName := L.CheckString(4)
	value := L.Get(5)
	includeFieldId := L.CheckBool(6)
	includeLength := false
	if L.GetTop() == 7 {
		includeLength = L.CheckBool(7)
	}

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	field := cls.GetFieldByName(fieldName)
	if field == dc.SwigcptrDCField(0) {
		L.ArgError(4, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", fieldName, clsName))
		return 0
	}
	packer.BeginPack(field)
	core.PackLuaValue(packer, value)
	if !packer.EndPack() {
		L.ArgError(5, "Pack failed!")
		return 0
	}

	packedData := packer.GetBytes()
	defer dc.DeleteVector(packedData)

	if includeFieldId {
		dg.AddUint16(uint16(field.GetNumber()))
	}
	if includeLength {
		dg.AddUint16(uint16(packedData.Size()))
	}
	dg.AddVector(packedData)
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
	client.client.SendDatagram(*dg)
	return 1
}

func LuaRouteDatagram(L *lua.LState) int {
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	client.RouteDatagram(*dg)
	return 1
}

func LuaGetDatabaseValues(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fieldsTable := L.CheckTable(4)
	callback := L.CheckFunction(5)

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	fields := make([]string, 0)
	fieldsTable.ForEach(func(_, l2 lua.LValue) {
		fieldName := l2.(lua.LString)
		fields = append(fields, string(fieldName))
	})

	callbackFunc := func(dbDoId Doid_t, dgi *DatagramIterator) {
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

		DCLock.Lock()

		packedValues := make([]dc.Vector, count)
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

			dcField := cls.GetFieldByName(field)
			if dcField == dc.SwigcptrDCField(0) {
				client.log.Warnf("GetStoredValues: Field \"%s\" does not exist for class \"%s\"", field, clsName)
				continue
			}

			if found {
				data := packedValues[i]
				// Validate that the data is correct
				if !dcField.ValidateRanges(data) {
					client.log.Errorf("GetStoredValues: Received invalid data for field \"%s\"!\n%s", field, DumpVector(data))
					continue
				}

				unpacker.SetUnpackData(data)
				unpacker.BeginUnpack(dcField)
				fieldTable.RawSetString(fields[i], core.UnpackDataToLuaValue(unpacker, L))
				unpacker.EndUnpack()

			}
		}
		DCLock.Unlock()

		// Cleanup
		for _, data := range packedValues {
			dc.DeleteVector(data)
		}

		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, fieldTable)
	}

	client.getDatabaseValues(doId, fields, callbackFunc)
	return 1
}

func LuaGetAllRequiredFromDatabase(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	callback := L.CheckFunction(4)

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	fields := make([]string, 0)
	for i := 0; i < cls.GetNumInheritedFields(); i++ {
		field := cls.GetInheritedField(i)
		if molecular, ok := field.AsMolecularField().(dc.DCMolecularField); ok {
			if molecular != dc.SwigcptrDCMolecularField(0) {
				continue
			}
		}
		if field.IsRequired() {
			fields = append(fields, field.GetName())
		}
	}

	callbackFunc := func(dbDoId Doid_t, dgi *DatagramIterator) {
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

		DCLock.Lock()

		packedValues := make([]dc.Vector, count)
		hasValue := map[string]bool{}
		for i := uint16(0); i < count; i++ {
			packedValues[i] = dgi.ReadVector()
			hasValue[fields[i]] = dgi.ReadBool()
			if !hasValue[fields[i]] {
				client.log.Debugf("GetStoredValues: Data for field \"%s\" not found, will be replaced with default value", fields[i])

			}
		}

		resultTable := L.NewTable()
		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		for i := uint16(0); i < count; i++ {
			field := fields[i]
			found := hasValue[field]

			dcField := cls.GetFieldByName(field)
			if dcField == dc.SwigcptrDCField(0) {
				client.log.Warnf("GetStoredValues: Field \"%s\" does not exist for class \"%s\"", field, clsName)
				if found {
					dc.DeleteVector(packedValues[i])
				}
				continue
			}

			var data dc.Vector
			if found {
				data = packedValues[i]
				// Validate that the data is correct
				if !dcField.ValidateRanges(data) {
					client.log.Errorf("GetStoredValues: Received invalid data for field \"%s\"!\n%s", field, DumpVector(data))
					dc.DeleteVector(data)
					continue
				}
			} else {
				// Get default value instead.
				value := dcField.GetDefaultValue()
				data = dc.NewVector()
				for i := int64(0); i < value.Size(); i++ {
					data.Add(value.Get(int(i)))
				}
			}

			unpacker.SetUnpackData(data)
			unpacker.BeginUnpack(dcField)
			fieldTable := L.NewTable()
			fieldTable.Append(lua.LString(fields[i]))
			fieldTable.Append(core.UnpackDataToLuaValue(unpacker, L))
			unpacker.EndUnpack()

			resultTable.Append(fieldTable)

			dc.DeleteVector(data)
		}
		DCLock.Unlock()
		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, resultTable)
	}

	client.getDatabaseValues(doId, fields, callbackFunc)
	return 1
}

func LuaQueryObjectFields(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fieldsTable := L.CheckTable(4)
	callback := L.CheckFunction(5)

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	fields := make([]string, 0)
	fieldsTable.ForEach(func(_, l2 lua.LValue) {
		fieldName := l2.(lua.LString)
		fields = append(fields, string(fieldName))
	})

	var fieldIds []uint16
	for _, fieldName := range fields {
		field := cls.GetFieldByName(fieldName)
		if field == dc.SwigcptrDCField(0) {
			client.log.Warnf("queryObjectFields: Class \"%s\" does not have field \"%s\"!", clsName, fieldName)
			continue
		}
		fieldIds = append(fieldIds, uint16(field.GetNumber()))
	}

	if len(fieldIds) == 0 {
		client.log.Warnf("queryObjectFields: Nothing to do for class \"%s\"!", clsName)
		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, client.ca.L.NewTable())
		return 1
	}

	callbackFunc := func(dgi *DatagramIterator) {
		success := dgi.ReadBool()
		if !success {
			client.log.Warnf("QueryFieldsResp returned unsuccessful for ID %d!", doId)
			client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LBool(success), lua.LNil)
			return
		}

		found := dgi.ReadUint16()
		client.log.Debugf("queryObjectFields: Found %d fields for %s(%d)", found, clsName, doId)

		fieldTable := client.ca.L.NewTable()

		DCLock.Lock()
		defer DCLock.Unlock()

		packedData := dgi.ReadRemainderAsVector()
		defer dc.DeleteVector(packedData)

		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		unpacker.SetUnpackData(packedData)
		for i := uint16(0); i < found; i++ {
			fieldId := unpacker.RawUnpackUint16().(uint)
			field := cls.GetFieldByIndex(int(fieldId))
			if field == dc.SwigcptrDCField(0) {
				client.log.Warnf("queryObjectFields: Unknown field %d for class \"%s\"!", fieldId, clsName)
				continue
			}
			unpacker.BeginUnpack(field)
			lValue := core.UnpackDataToLuaValue(unpacker, client.ca.L)
			if !unpacker.EndUnpack() {
				client.log.Warnf("queryObjectFields: Unable to unpack field \"%s\"!\n%s", field.GetName(), DumpUnpacker(unpacker))
				continue
			}
			fieldTable.RawSetString(field.GetName(), lValue)
		}

		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, fieldTable)
	}

	context := client.queryFieldsContextMap.Set(client.context.Add(1), callbackFunc, true)
	defer client.queryFieldsContextMap.Unlock()

	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(doId), client.channel, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddDoid(doId)
	dg.AddUint32(context)
	for _, fieldId := range fieldIds {
		dg.AddUint16(fieldId)
	}
	client.RouteDatagram(dg)
	return 1
}

func LuaQueryAllRequiredFields(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	callback := L.CheckFunction(4)

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class not found.")
		return 0
	}

	var fieldIds []uint16
	for i := 0; i < cls.GetNumInheritedFields(); i++ {
		field := cls.GetInheritedField(i)
		if field.IsRequired() {
			fieldIds = append(fieldIds, uint16(field.GetNumber()))
		}
	}

	if len(fieldIds) == 0 {
		client.log.Warnf("queryObjectFields: Nothing to do for class \"%s\"!", clsName)
		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, client.ca.L.NewTable())
		return 1
	}

	callbackFunc := func(dgi *DatagramIterator) {
		success := dgi.ReadBool()
		if !success {
			client.log.Warnf("QueryFieldsResp returned unsuccessful for ID %d!", doId)
			client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LBool(success), lua.LNil)
			return
		}

		found := dgi.ReadUint16()
		client.log.Debugf("queryObjectFields: Found %d fields for %s(%d)", found, clsName, doId)

		resultTable := client.ca.L.NewTable()

		DCLock.Lock()
		defer DCLock.Unlock()

		packedData := dgi.ReadRemainderAsVector()
		defer dc.DeleteVector(packedData)

		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		unpacker.SetUnpackData(packedData)
		for i := uint16(0); i < found; i++ {
			fieldId := unpacker.RawUnpackUint16().(uint)
			field := cls.GetFieldByIndex(int(fieldId))
			if field == dc.SwigcptrDCField(0) {
				client.log.Warnf("queryObjectFields: Unknown field %d for class \"%s\"!", fieldId, clsName)
				continue
			}
			unpacker.BeginUnpack(field)
			lValue := core.UnpackDataToLuaValue(unpacker, client.ca.L)
			if !unpacker.EndUnpack() {
				client.log.Warnf("queryObjectFields: Unable to unpack field \"%s\"!\n%s", field.GetName(), DumpUnpacker(unpacker))
				continue
			}
			fieldTable := client.ca.L.NewTable()
			fieldTable.Append(lua.LString(field.GetName()))
			fieldTable.Append(lValue)

			resultTable.Append(fieldTable)
		}

		client.ca.CallLuaFunction(callback, client, lua.LNumber(doId), lua.LTrue, resultTable)
	}
	context := client.queryFieldsContextMap.Set(client.context.Add(1), callbackFunc, true)
	defer client.queryFieldsContextMap.Unlock()

	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(doId), client.channel, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddDoid(doId)
	dg.AddUint32(context)
	for _, fieldId := range fieldIds {
		dg.AddUint16(fieldId)
	}
	client.RouteDatagram(dg)
	return 1
}

func LuaSetDatabaseValues(L *lua.LState) int {
	client := CheckClient(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fields := L.CheckTable(4)

	cls := core.DC.GetClassByName(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(2, "Class not found.")
		return 0
	}

	DCLock.Lock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedFields := map[string]dc.Vector{}
	// TODO: string dictionary sanity check
	fields.ForEach(func(l1, data lua.LValue) {
		name := string(l1.(lua.LString))
		field := cls.GetFieldByName(name)
		if field == dc.SwigcptrDCField(0) {
			L.ArgError(3, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", name, clsName))
			return
		}
		packer.BeginPack(field)
		core.PackLuaValue(packer, data)
		if !packer.EndPack() {
			L.ArgError(3, "Pack failed!")
			return
		}

		packedFields[name] = packer.GetBytes()
		packer.ClearData()
	})

	DCLock.Unlock()
	client.setDatabaseValues(doId, packedFields)

	return 1
}

func LuaGetSetUserTable(L *lua.LState) int {
	client := CheckClient(L, 1)
	if L.GetTop() == 2 {
		table := L.CheckTable(2)
		client.userTable = table
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
			zone := dgi.ReadZone()
			if !slices.Contains(zones, zone) {
				zones = append(zones, zone)
			}
		}
	} else {
		// client:handleAddInterest(handle, context, parent, {zone...})
		handle = uint16(L.CheckInt(2))
		context = uint32(L.CheckInt(3))
		parent = Doid_t(L.CheckInt(4))
		zonesTable := L.CheckTable(5)

		zonesTable.ForEach(func(_, l2 lua.LValue) {
			zone := Zone_t(l2.(lua.LNumber))
			if !slices.Contains(zones, zone) {
				zones = append(zones, zone)
			}
		})
	}

	i := client.buildInterest(handle, parent, zones)

	client.Lock()
	defer client.Unlock()

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
		client.Lock()
		defer client.Unlock()

		client.removeInterest(i, context)
	} else {
		client.log.Debugf("Attempted to remove non-existant interest: %d", handle)
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
		channel = Channel_t(account)<<32 | Channel_t(avatar)
	}
	client.SetChannel(channel)
	return 1
}

func LuaSubscribePuppetChannel(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Channel_t(L.CheckInt(2))
	puppetType := Channel_t(L.CheckInt(3))

	client.SubscribeChannel(do + puppetType<<32)
	return 1
}

func LuaUnsubscribePuppetChannel(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Channel_t(L.CheckInt(2))
	puppetType := Channel_t(L.CheckInt(3))

	client.UnsubscribeChannel(do + puppetType<<32)
	return 1
}

func LuaHandleUpdateField(L *lua.LState) int {
	client := CheckClient(L, 1)
	dgi := CheckDatagramIterator(L, 2)

	do, field := dgi.ReadDoid(), dgi.ReadUint16()
	client.handleClientUpdateField(do, field, dgi)

	return 1
}

func LuaSendActivateObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))
	className := L.CheckString(3)

	var fields *lua.LTable
	if L.GetTop() == 4 {
		fields = L.CheckTable(4)
	}

	dclass := core.DC.GetClassByName(className)
	if dclass == dc.SwigcptrDCClass(0) {
		L.ArgError(3, "Class does not exist.")
		return 0
	}

	dg := NewDatagram()
	if fields != nil {
		dg.AddServerHeader(Channel_t(do), client.channel, STATESERVER_OBJECT_CREATE_WITH_REQUIR_OTHER_CONTEXT)
	} else {
		dg.AddServerHeader(Channel_t(do), client.channel, STATESERVER_OBJECT_CREATE_WITH_REQUIRED_CONTEXT)
	}
	dg.AddDoid(do)
	dg.AddLocation(0, 0)
	dg.AddUint16(uint16(dclass.GetNumber()))

	if fields != nil {
		DCLock.Lock()
		defer DCLock.Unlock()

		packer := dc.NewDCPacker()
		defer dc.DeleteDCPacker(packer)

		length := uint16(0)
		fields.ForEach(func(l1, data lua.LValue) {
			name := string(l1.(lua.LString))
			field := dclass.GetFieldByName(name)
			if field == dc.SwigcptrDCField(0) {
				L.ArgError(4, fmt.Sprintf("Field \"%s\" not found in class \"%s\"", name, className))
				return
			}
			length++
			packer.RawPackUint16(uint(field.GetNumber()))
			packer.BeginPack(field)
			core.PackLuaValue(packer, data)
			if !packer.EndPack() {
				L.ArgError(4, "Pack failed!")
				return
			}

		})
		packedData := packer.GetBytes()
		dg.AddUint16(length)
		dg.AddVector(packedData)

		dc.DeleteVector(packedData)
		packer.ClearData()
	}

	client.RouteDatagram(dg)
	return 1
}

func LuaObjectSetOwner(L *lua.LState) int {
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

func LuaRemoveSessionObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))

	for _, d := range client.sessionObjects {
		if d == do {
			break
		}
		client.log.Warnf("Received remove sesion object with non-existant ID=%d", do)
	}

	client.log.Debugf("Removed session object with ID %d", do)

	tempSessionObjectSlice := make([]Doid_t, 0)
	for _, o := range client.sessionObjects {
		if o != do {
			tempSessionObjectSlice = append(tempSessionObjectSlice, o)
		}
	}
	client.sessionObjects = tempSessionObjectSlice
	return 1
}

func LuaAddPostRemove(L *lua.LState) int {
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)

	client.AddPostRemove(*dg)
	return 1
}

func LuaClearPostRemoves(L *lua.LState) int {
	client := CheckClient(L, 1)

	client.ClearPostRemoves()
	return 1
}

func LuaDeclareObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)

	if _, ok := client.declaredObjects[do]; ok {
		client.log.Warnf("Received object declaration for previously declared object %d", do)
		return 1
	}

	cls := core.DC.GetClassByName(clsName)
	client.declaredObjects[do] = DeclaredObject{
		do: do,
		dc: cls,
	}
	return 1
}

func LuaUndeclareObject(L *lua.LState) int {
	client := CheckClient(L, 1)
	do := Doid_t(L.CheckInt(2))

	if _, ok := client.declaredObjects[do]; !ok {
		client.log.Warnf("Received object de-declaration for previously declared object %d", do)
		return 1
	}

	delete(client.declaredObjects, do)
	return 1
}

func LuaUndeclareAllObjects(L *lua.LState) int {
	client := CheckClient(L, 1)
	clear(client.declaredObjects)
	return 1
}

func LuaSetLocation(L *lua.LState) int {
	client := CheckClient(L, 1)

	var do Doid_t
	var parent Doid_t
	var zone Zone_t

	if L.GetTop() == 2 {
		// client:setLocation(dgi)
		dgi := CheckDatagramIterator(L, 2)

		do = dgi.ReadDoid()
		parent = dgi.ReadDoid()
		zone = dgi.ReadZone()
	} else {
		// client:setLocation(do, parent, zone)
		do = Doid_t(L.CheckInt(2))
		parent = Doid_t(L.CheckInt(3))
		zone = Zone_t(L.CheckInt(4))
	}

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

func LuaWriteServerEvent(L *lua.LState) int {
	client := CheckClient(L, 1)
	eventType := L.CheckString(2)
	serverName := L.CheckString(3)
	description := L.CheckString(4)

	event := eventlogger.NewLoggedEvent(eventType, serverName, strconv.FormatUint(uint64(client.allocatedChannel), 10), description)
	event.Send()

	return 1
}
