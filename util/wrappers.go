package util

import (
	"fmt"

	"github.com/yuin/gopher-lua"
)

// Util wrappers for Lua.

const luaDatagramTypeName = "datagram"
const luaDgiTypeName = "dgi"

func RegisterDatagramType(L *lua.LState)  {
	mt := L.NewTypeMetatable(luaDatagramTypeName)
	L.SetGlobal(luaDatagramTypeName, mt)
	// Static attributes
	L.SetField(mt, "new", L.NewFunction(NewLuaDatagram))
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), DatagramMethods))
}

func NewLuaDatagram(L *lua.LState) int {
	dg := NewDatagram()
	ud := L.NewUserData()
	ud.Value = dg
	L.SetMetatable(ud, L.GetTypeMetatable(luaDatagramTypeName))
	L.Push(ud)
	return 1
}

func CheckDatagram(L *lua.LState, n int) *Datagram {
	ud := L.CheckUserData(n)
	if dg, ok := ud.Value.(Datagram); ok {
		return &dg
	}
	L.ArgError(n, "Datagram expected")
	return nil
}

func RegisterDatagramIteratorType(L *lua.LState)  {
	mt := L.NewTypeMetatable(luaDgiTypeName)
	L.SetGlobal(luaDgiTypeName, mt)
	// Static attributes
	L.SetField(mt, "new", L.NewFunction(NewLuaDatagramIteratorFromLua))
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), DatagramIteratorMethods))
}

func NewLuaDatagramIteratorFromExisting(L *lua.LState, dgi *DatagramIterator) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = dgi
	L.SetMetatable(ud, L.GetTypeMetatable(luaDgiTypeName))
	return ud
}

func NewLuaDatagramIteratorFromLua(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	dgi := NewDatagramIterator(dg)
	ud := NewLuaDatagramIteratorFromExisting(L, dgi)
	L.Push(ud)
	return 1
}

func NewLuaDatagramIteratorFromDatagram(L *lua.LState, dg *Datagram) *lua.LUserData {
	dgi := NewDatagramIterator(dg)
	return NewLuaDatagramIteratorFromExisting(L, dgi)
}

func CheckDatagramIterator(L *lua.LState, n int) *DatagramIterator {
	ud := L.CheckUserData(n)
	if dgi, ok := ud.Value.(*DatagramIterator); ok {
		return dgi
	}
	L.ArgError(n, "DatagramIterator expected")
	return nil
}

var DatagramMethods = map[string]lua.LGFunction{
	"addServerHeader": LuaAddServerHeader,
	"addInt8": LuaAddInt8,
	"addUint8": LuaAddUint8,
	"addInt16": LuaAddInt16,
	"addUint16": LuaAddUint16,
	"addInt32": LuaAddInt32,
	"addUint32": LuaAddUint32,
	"addInt64": LuaAddInt64,
	"addBool": LuaAddBool,
	"addString": LuaAddString,
	"addData": LuaAddData,
}

func LuaAddServerHeader(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	to := L.Get(2)
	from := Channel_t(L.CheckInt(3))
	messageType := uint16(L.CheckInt(4))

	if to.Type() == lua.LTNumber {
		dg.AddServerHeader(Channel_t(to.(lua.LNumber)), from, messageType)
	} else if to.Type() == lua.LTTable {
		luaChannels := to.(*lua.LTable)
		channels := []Channel_t{}
		luaChannels.ForEach(func(l1, l2 lua.LValue) {
			if channel, ok := l2.(lua.LNumber); ok {
				channels = append(channels, Channel_t(channel))
			}
		})
		dg.AddMultipleServerHeader(channels, from, messageType)
	} else {
		L.ArgError(2, fmt.Sprintf("Got type %s, expecting number or table", to.Type().String()))
		return 0
	}
	return 1
}

func LuaAddInt8(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddInt8(int8(v))
	return 1
}

func LuaAddUint8(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddUint8(uint8(v))
	return 1
}

func LuaAddInt16(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddInt16(int16(v))
	return 1
}

func LuaAddUint16(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddUint16(uint16(v))
	return 1
}

func LuaAddInt32(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddInt32(int32(v))
	return 1
}

func LuaAddUint32(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt(2)
	dg.AddUint32(uint32(v))
	return 1
}

func LuaAddInt64(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckInt64(2)
	dg.AddInt64(v)
	return 1
}

func LuaAddBool(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckBool(2)
	dg.AddBool(v)
	return 1
}

func LuaAddString(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckString(2)
	dg.AddString(v)
	return 1
}

func LuaAddData(L * lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckString(2)
	dg.AddData([]byte(v))
	return 1
}

var DatagramIteratorMethods = map[string]lua.LGFunction{
	"dumpHex": LuaDgiDumpHex,
	"getRemainingSize": LuaGetRemainingSize,
	"readInt8": LuaReadInt8,
	"readUint8": LuaReadUint8,
	"readInt16": LuaReadInt16,
	"readUint16": LuaReadUint16,
	"readInt32": LuaReadInt32,
	"readUint32": LuaReadUint32,
	"readInt64": LuaReadInt64,
	"readUint64": LuaReadUint64,
	"readBool": LuaReadBool,
	"readString": LuaReadString,
	"readRemainder": LuaReadRemainder,
	"readFixedString": LuaReadFixedString,
}

func LuaDgiDumpHex(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	L.Push(lua.LString(dgi.String()))
	return 1
}

func LuaReadInt8(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadInt8()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadUint8(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadUint8()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadInt16(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadInt16()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadUint16(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadUint16()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadInt32(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadInt32()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadUint32(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadUint32()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadInt64(L *lua.LState) int {
	// NOTE: Lua has no native int64 type support.
	// Use at your own risk.
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadInt64()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadUint64(L *lua.LState) int {
	// NOTE: Lua has no native uint64 type support.
	// Use at your own risk.
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadUint64()
	L.Push(lua.LNumber(v))
	return 1
}

func LuaReadBool(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadBool()
	L.Push(lua.LBool(v))
	return 1
}

func LuaReadString(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadString()
	L.Push(lua.LString(v))
	return 1
}

func LuaGetRemainingSize(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	size := dgi.RemainingSize()
	L.Push(lua.LNumber(size))
	return 1
}

func LuaReadRemainder(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	remainder := dgi.ReadRemainder()
	L.Push(lua.LString(remainder))
	return 1
}

func LuaReadFixedString(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	size := L.CheckInt(2)
	v := dgi.ReadData(Dgsize_t(size))
	L.Push(lua.LString(v))
	return 1
}
