package luarole

import (
	// "otpgo/core"
	"otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"

	dc "github.com/LittleToonCat/dcparser-go"
	"github.com/yuin/gopher-lua"
	"fmt"
)

// Participant wrappers for Lua

const luaParticipantType = "participant"

func RegisterLuaParticipantType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaParticipantType)
	L.SetGlobal(luaParticipantType, mt)
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), ParticipantMethods))
}

func NewLuaParticipant(L *lua.LState, participant *LuaRole) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = participant
	L.SetMetatable(ud, L.GetTypeMetatable(luaParticipantType))
	return ud
}

func CheckParticipant(L *lua.LState, n int) *LuaRole {
	ud := L.CheckUserData(n)
	if participant, ok := ud.Value.(*LuaRole); ok {
		return participant
	}
	L.ArgError(n, "LuaRole expected")
	return nil
}

var ParticipantMethods = map[string]lua.LGFunction{
	"subscribeChannel": LuaSubscribeChannel,
	"unsubscribeChannel": LuaUnsubscribeChannel,
	"subscribeRange": LuaSubscribeRange,
	"unsubscribeRange": LuaUnsubscribeRange,
	"handleUpdateField": LuaHandleUpdateField,
	"addServerHeaderWithAvatarId": LuaAddServerHeaderWithAvatarId,
	"getSender": LuaGetSender,
	"getAccountIdFromSender": LuaGetAccountIdFromSender,
	"getAvatarIdFromSender": LuaGetAvatarIdFromSender,
	"sendUpdate": LuaSendUpdate,
	"sendUpdateToAvatarId": LuaSendUpdateToAvatarId,
	"sendUpdateToAccountId": LuaSendUpdateToAccountId,
	"queryObjectFields": LuaQueryObjectFields,
	"setDatabaseValues": LuaSetDatabaseValues,
	"routeDatagram": LuaRouteDatagram,
}

func LuaSubscribeChannel(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	channel := Channel_t(L.CheckInt(2))
	participant.SubscribeChannel(channel)
	return 1
}

func LuaUnsubscribeChannel(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	channel := Channel_t(L.CheckInt(2))
	participant.UnsubscribeChannel(channel)
	return 1
}

func LuaSubscribeRange(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	min := Channel_t(L.CheckInt(2))
	max := Channel_t(L.CheckInt(3))
	participant.SubscribeRange(messagedirector.Range{Min: min, Max: max})
	return 1
}

func LuaUnsubscribeRange(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	min := Channel_t(L.CheckInt(2))
	max := Channel_t(L.CheckInt(3))
	participant.UnsubscribeRange(messagedirector.Range{Min: min, Max: max})
	return 1
}

func LuaHandleUpdateField(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	dgi := CheckDatagramIterator(L, 2)
	className := L.CheckString(3)

	participant.handleUpdateField(dgi, className)
	return 1
}

func LuaGetSender(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	L.Push(lua.LNumber(participant.sender))
	return 1
}

func LuaGetAccountIdFromSender(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	L.Push(lua.LNumber(participant.sender >> 32))
	return 1
}

func LuaGetAvatarIdFromSender(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	L.Push(lua.LNumber(participant.sender & 0xffffffff))
	return 1
}

func LuaRouteDatagram(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	dg := CheckDatagram(L, 2)
	go participant.RouteDatagram(*dg)
	return 1
}

func LuaAddServerHeaderWithAvatarId(L *lua.LState) int {
	// This exists because of Lua cannot add
	// the avatar puppet channel on its own.
	dg := CheckDatagram(L, 2)
	avatarId := L.CheckInt(3)
	sender := Channel_t(L.CheckInt(4))
	msgType := uint16(L.CheckInt(5))

	dg.AddServerHeader(Channel_t(avatarId + (1 << 32)), sender, msgType)
	return 1
}

func LuaSendUpdate(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	doId := Doid_t(L.CheckInt(2))
	from := L.CheckInt(3)
	className := L.CheckString(4)
	fieldName := L.CheckString(5)
	v := L.Get(6)

	participant.sendUpdateToChannel(Channel_t(doId), Doid_t(from), className, fieldName, v)
	return 1
}

func LuaSendUpdateToAvatarId(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	avatarId := L.CheckInt(2)
	from := L.CheckInt(3)
	className := L.CheckString(4)
	fieldName := L.CheckString(5)
	v := L.Get(6)

	participant.sendUpdateToChannel(Channel_t(avatarId + (1 << 32)), Doid_t(from), className, fieldName, v)
	return 1
}

func LuaSendUpdateToAccountId(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	accountId := L.CheckInt(2)
	from := L.CheckInt(3)
	className := L.CheckString(4)
	fieldName := L.CheckString(5)
	v := L.Get(6)

	participant.sendUpdateToChannel(Channel_t(accountId + (3 << 32)), Doid_t(from), className, fieldName, v)
	return 1
}

func LuaQueryObjectFields(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	doId := Doid_t(L.CheckInt(2))
	clsName := L.CheckString(3)
	fieldsTable := L.CheckTable(4)
	from := Channel_t(L.CheckInt(5))
	callback := L.CheckFunction(6)

	senderContext := participant.sender

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

	var fieldIds []uint16
	for _, fieldName := range fields {
		field := cls.Get_field_by_name(fieldName)
		if field == dc.SwigcptrDCField(0) {
			participant.log.Warnf("queryObjectFields: Class \"%s\" does not have field \"%s\"!", clsName, fieldName)
			continue
		}
		fieldIds = append(fieldIds, uint16(field.Get_number()))
	}

	if len(fieldIds) == 0 {
		participant.log.Warnf("queryObjectFields: Nothing to do for class \"%s\"!", clsName)
		go participant.CallLuaFunction(callback, senderContext, lua.LNumber(doId), lua.LTrue, participant.L.NewTable())
		return 1
	}

	callbackFunc := func(dgi *DatagramIterator) {
		success := dgi.ReadBool()
		if !success {
			participant.log.Warnf("QueryFieldsResp returned unsuccessful for ID %d!", doId)
			go participant.CallLuaFunction(callback, senderContext, lua.LNumber(doId), lua.LBool(success), lua.LNil)
			return
		}

		found := dgi.ReadUint16()
		participant.log.Debugf("queryObjectFields: Found %d fields for %s(%d)", found, clsName, doId)

		fieldTable := participant.L.NewTable()

		DCLock.Lock()
		defer DCLock.Unlock()

		packedData := dgi.ReadRemainderAsVector()
		defer dc.DeleteVector_uchar(packedData)

		unpacker := dc.NewDCPacker()
		defer dc.DeleteDCPacker(unpacker)

		unpacker.Set_unpack_data(packedData)
		for i := uint16(0); i < found; i++ {
			fieldId := unpacker.Raw_unpack_uint16().(uint)
			field := cls.Get_field_by_index(int(fieldId))
			if field == dc.SwigcptrDCField(0) {
				participant.log.Warnf("queryObjectFields: Unknown field %d for class \"%s\"!", fieldId, clsName)
				continue
			}
			unpacker.Begin_unpack(field)
			lValue := core.UnpackDataToLuaValue(unpacker, participant.L)
			if !unpacker.End_unpack() {
				participant.log.Warnf("queryObjectFields: Unable to unpack field \"%s\"!\n%s", field.Get_name(), DumpUnpacker(unpacker))
				continue
			}
			fieldTable.RawSetString(field.Get_name(), lValue)
		}

		go participant.CallLuaFunction(callback, senderContext, lua.LNumber(doId), lua.LTrue, fieldTable)
	}

	participant.queryFieldsContextMap[participant.context] = callbackFunc

	dg := NewDatagram()
	dg.AddServerHeader(Channel_t(doId), from, STATESERVER_OBJECT_QUERY_FIELDS)
	dg.AddUint32(participant.context)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fieldIds)))
	for _, fieldId := range fieldIds {
		dg.AddUint16(fieldId)
	}
	participant.RouteDatagram(dg)
	participant.context++
	return 1
}

func LuaSetDatabaseValues(L *lua.LState) int {
	participant := CheckParticipant(L, 1)
	doId := Doid_t(L.CheckInt(2))
	dbChannel := Channel_t(L.CheckInt(3))
	clsName := L.CheckString(4)
	fields := L.CheckTable(5)

	cls := core.DC.Get_class_by_name(clsName)
	if cls == dc.SwigcptrDCClass(0) {
		L.ArgError(2, "Class not found.")
		return 0
	}

	DCLock.Lock()

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

	DCLock.Unlock()
	participant.setDatabaseValues(doId, dbChannel, packedFields)

	return 1
}
