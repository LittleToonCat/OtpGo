package clientagent

import (
    "github.com/yuin/gopher-lua"
	. "otpgo/util"
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
	"sendDisconnect": LuaSendDisconnect,
	"handleHeartbeat": LuaHandleHeartbeat,
	"handleDisconnect": LuaHandleDisconnect,
	"sendDatagram": LuaSendDatagram,
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

func LuaSendDisconnect(L *lua.LState) int {
	client := CheckClient(L, 1)
	reason := L.CheckInt(2)
	error := L.CheckString(3)
	security := L.CheckBool(4)

	client.sendDisconnect(uint16(reason), error, security)
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
	client.handleDisconnect()
	return 1
}

func LuaSendDatagram(L *lua.LState) int {
	client := CheckClient(L, 1)
	dg := CheckDatagram(L, 2)
	go client.client.SendDatagram(*dg)
	return 1
}
