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
	"authenticated": LuaGetSetAuthenticated,
	"createDatabaseObject": LuaCreateDatabaseObject,
	"getDatabaseValues": LuaGetDatabaseValues,
	"handleHeartbeat": LuaHandleHeartbeat,
	"handleDisconnect": LuaHandleDisconnect,
	"sendDatagram": LuaSendDatagram,
	"sendDisconnect": LuaSendDisconnect,
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
