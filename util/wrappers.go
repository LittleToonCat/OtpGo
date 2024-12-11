package util

import (
	"fmt"

	"github.com/yuin/gopher-lua"
)

// Util wrappers for Lua.

const luaDatagramTypeName = "datagram"
const luaDgiTypeName = "datagramiterator"
const luaInt64TypeName = "int64"
const luaUint64TypeName = "uint64"

func RegisterDatagramType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaDatagramTypeName)
	L.SetGlobal(luaDatagramTypeName, mt)
	// Static attributes
	L.SetField(mt, "new", L.NewFunction(NewLuaDatagram))
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), DatagramMethods))
	L.SetField(mt, "__tostring", L.NewFunction(LuaDatagramToString))
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

func RegisterDatagramIteratorType(L *lua.LState) {
	mt := L.NewTypeMetatable(luaDgiTypeName)
	L.SetGlobal(luaDgiTypeName, mt)
	// Static attributes
	L.SetField(mt, "new", L.NewFunction(NewLuaDatagramIteratorFromLua))
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), DatagramIteratorMethods))
	L.SetField(mt, "__tostring", L.NewFunction(LuaDgiToString))
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

func RegisterInt64Type(L *lua.LState) {
	mt := L.NewTypeMetatable(luaInt64TypeName)
	L.SetGlobal(luaInt64TypeName, mt)
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), Int64Methods))
	L.SetField(mt, "__tostring", L.NewFunction(Int64ToString))
	L.SetField(mt, "__add", L.NewFunction(Int64Add))
	L.SetField(mt, "__sub", L.NewFunction(Int64Subtract))
	L.SetField(mt, "__eq", L.NewFunction(Int64Equal))
}

func NewLuaInt64(L *lua.LState, i int64) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = i
	L.SetMetatable(ud, L.GetTypeMetatable(luaInt64TypeName))
	return ud
}

func CheckInt64(L *lua.LState, n int) int64 {
	ud := L.CheckUserData(n)
	if i, ok := ud.Value.(int64); ok {
		return i
	}
	L.ArgError(n, "Int64 expected")
	return 0
}

func RegisterUint64Type(L *lua.LState) {
	mt := L.NewTypeMetatable(luaUint64TypeName)
	L.SetGlobal(luaUint64TypeName, mt)
	// Methods
	L.SetField(mt, "__index", L.SetFuncs(L.NewTable(), Uint64Methods))
	L.SetField(mt, "__tostring", L.NewFunction(Uint64ToString))
	L.SetField(mt, "__add", L.NewFunction(Uint64Add))
	L.SetField(mt, "__sub", L.NewFunction(Uint64Subtract))
	L.SetField(mt, "__eq", L.NewFunction(Uint64Equal))
}

func NewLuaUint64(L *lua.LState, i uint64) *lua.LUserData {
	ud := L.NewUserData()
	ud.Value = i
	L.SetMetatable(ud, L.GetTypeMetatable(luaUint64TypeName))
	return ud
}

func CheckUint64(L *lua.LState, n int) uint64 {
	ud := L.CheckUserData(n)
	if i, ok := ud.Value.(uint64); ok {
		return i
	}
	L.ArgError(n, "Uint64 expected")
	return 0
}

func RegisterLuaUtilTypes(L *lua.LState) {
	RegisterInt64Type(L)
	RegisterUint64Type(L)
	RegisterDatagramType(L)
	RegisterDatagramIteratorType(L)
}

var DatagramMethods = map[string]lua.LGFunction{
	"addServerHeader": LuaAddServerHeader,
	"addInt8":         LuaAddInt8,
	"addUint8":        LuaAddUint8,
	"addInt16":        LuaAddInt16,
	"addUint16":       LuaAddUint16,
	"addInt32":        LuaAddInt32,
	"addUint32":       LuaAddUint32,
	"addInt64":        LuaAddInt64,
	"addUint64":       LuaAddUint64,
	"addBool":         LuaAddBool,
	"addString":       LuaAddString,
	"addData":         LuaAddData,
	"addDatagram":     LuaAddDatagram,
	"addBlob":         LuaAddBlob,
}

func LuaDatagramToString(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	L.Push(lua.LString(dg.String()))
	return 1
}

func LuaAddServerHeader(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	to := L.Get(2)
	from := Channel_t(L.CheckInt(3))
	messageType := uint16(L.CheckInt(4))

	if to.Type() == lua.LTNumber {
		dg.AddServerHeader(Channel_t(to.(lua.LNumber)), from, messageType)
	} else if to.Type() == lua.LTUserData {
		if channel, ok := to.(*lua.LUserData).Value.(uint64); ok {
			dg.AddServerHeader(Channel_t(channel), from, messageType)
		}
	} else if to.Type() == lua.LTTable {
		luaChannels := to.(*lua.LTable)
		channels := []Channel_t{}
		luaChannels.ForEach(func(l1, l2 lua.LValue) {
			if channel, ok := l2.(lua.LNumber); ok {
				channels = append(channels, Channel_t(channel))
			} else if channel, ok := l2.(*lua.LUserData).Value.(uint64); ok {
				channels = append(channels, Channel_t(channel))
			}
		})
		dg.AddMultipleServerHeader(channels, from, messageType)
	} else {
		L.ArgError(2, fmt.Sprintf("Got type %s, expecting Uint64, number or table{Uint64, number}", to.Type().String()))
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
	v := L.Get(2)

	switch v.Type() {
	case lua.LTUserData:
		if v, ok := v.(*lua.LUserData).Value.(int64); ok {
			dg.AddInt64(v)
		} else {
			L.ArgError(2, "UserData is not Int64")
		}
	case lua.LTNumber:
		dg.AddInt64(int64(v.(lua.LNumber)))
	default:
		L.ArgError(2, "Int64 or number expected.")
	}
	return 1
}

func LuaAddUint64(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.Get(2)

	switch v.Type() {
	case lua.LTUserData:
		if v, ok := v.(*lua.LUserData).Value.(uint64); ok {
			dg.AddUint64(v)
		} else {
			L.ArgError(2, "UserData is not Uint64")
		}
	case lua.LTNumber:
		dg.AddUint64(uint64(v.(lua.LNumber)))
	default:
		L.ArgError(2, "Uint64 or number expected.")
	}
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

func LuaAddData(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := L.CheckString(2)
	dg.AddData([]byte(v))
	return 1
}

func LuaAddDatagram(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := CheckDatagram(L, 2)
	dg.AddDatagram(v)
	return 1
}

func LuaAddBlob(L *lua.LState) int {
	dg := CheckDatagram(L, 1)
	v := CheckDatagram(L, 2)
	dg.AddBlob(v)
	return 1
}

var DatagramIteratorMethods = map[string]lua.LGFunction{
	"getRemainingSize": LuaGetRemainingSize,
	"readInt8":         LuaReadInt8,
	"readUint8":        LuaReadUint8,
	"readInt16":        LuaReadInt16,
	"readUint16":       LuaReadUint16,
	"readInt32":        LuaReadInt32,
	"readUint32":       LuaReadUint32,
	"readInt64":        LuaReadInt64,
	"readUint64":       LuaReadUint64,
	"readBool":         LuaReadBool,
	"readString":       LuaReadString,
	"readRemainder":    LuaReadRemainder,
	"readFixedString":  LuaReadFixedString,
}

func LuaDgiToString(L *lua.LState) int {
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
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadInt64()
	L.Push(NewLuaInt64(L, v))
	return 1
}

func LuaReadUint64(L *lua.LState) int {
	dgi := CheckDatagramIterator(L, 1)
	v := dgi.ReadUint64()
	L.Push(NewLuaUint64(L, v))
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

var Int64Methods = map[string]lua.LGFunction{
	"tonumber": Int64ToNumber,
}

func Int64ToString(L *lua.LState) int {
	i := CheckInt64(L, 1)

	L.Push(lua.LString(fmt.Sprintf("Int64(%d)", i)))
	return 1
}

func Int64Add(L *lua.LState) int {
	i := CheckInt64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherInt64, ok := other.(*lua.LUserData).Value.(int64); ok {
			L.Push(NewLuaInt64(L, i+otherInt64))
		} else {
			L.Push(NewLuaInt64(L, i))
		}
	case lua.LTNumber:
		L.Push(NewLuaInt64(L, i+int64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Int64 or number expected.")
	}
	return 1
}

func Int64Subtract(L *lua.LState) int {
	i := CheckInt64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherInt64, ok := other.(*lua.LUserData).Value.(int64); ok {
			L.Push(NewLuaInt64(L, i-otherInt64))
		} else {
			L.Push(NewLuaInt64(L, i))
		}
	case lua.LTNumber:
		L.Push(NewLuaInt64(L, i-int64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Int64 or number expected.")
	}
	return 1
}

func Int64Equal(L *lua.LState) int {
	i := CheckInt64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherInt64, ok := other.(*lua.LUserData).Value.(int64); ok {
			L.Push(lua.LBool(i == otherInt64))
		} else {
			L.Push(lua.LBool(false))
		}
	case lua.LTNumber:
		L.Push(lua.LBool(i == int64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Int64 or number expected.")
	}
	return 1
}

func Int64ToNumber(L *lua.LState) int {
	i := CheckInt64(L, 1)
	L.Push(lua.LNumber(i))
	return 1
}

var Uint64Methods = map[string]lua.LGFunction{
	"tonumber": Uint64ToNumber,
}

func Uint64ToString(L *lua.LState) int {
	i := CheckUint64(L, 1)

	L.Push(lua.LString(fmt.Sprintf("Uint64(%d)", i)))
	return 1
}

func Uint64Add(L *lua.LState) int {
	i := CheckUint64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherUint64, ok := other.(*lua.LUserData).Value.(uint64); ok {
			L.Push(NewLuaUint64(L, i+otherUint64))
		} else {
			L.Push(NewLuaUint64(L, i))
		}
	case lua.LTNumber:
		L.Push(NewLuaUint64(L, i+uint64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Uint64 or number expected.")
	}
	return 1
}

func Uint64Subtract(L *lua.LState) int {
	i := CheckUint64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherUint64, ok := other.(*lua.LUserData).Value.(uint64); ok {
			L.Push(NewLuaUint64(L, i-otherUint64))
		} else {
			L.Push(NewLuaUint64(L, i))
		}
	case lua.LTNumber:
		L.Push(NewLuaUint64(L, i-uint64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Uint64 or number expected.")
	}
	return 1
}

func Uint64Equal(L *lua.LState) int {
	i := CheckUint64(L, 1)
	other := L.Get(2)

	switch other.Type() {
	case lua.LTUserData:
		if otherUint64, ok := other.(*lua.LUserData).Value.(uint64); ok {
			L.Push(lua.LBool(i == otherUint64))
		} else {
			L.Push(lua.LBool(false))
		}
	case lua.LTNumber:
		L.Push(lua.LBool(i == uint64(other.(lua.LNumber))))
	default:
		L.ArgError(2, "Uint64 or number expected.")
	}
	return 1
}

func Uint64ToNumber(L *lua.LState) int {
	i := CheckUint64(L, 1)
	L.Push(lua.LNumber(i))
	return 1
}
