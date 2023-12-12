package luarole

import (
	// "otpgo/core"
	"otpgo/messagedirector"
	. "otpgo/util"

	"github.com/yuin/gopher-lua"
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
	if participaint, ok := ud.Value.(*LuaRole); ok {
		return participaint
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
	"getAccountIdFromSender": LuaGetAccountIdFromSender,
	"getAvatarIdFromSender": LuaGetAvatarIdFromSender,
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
