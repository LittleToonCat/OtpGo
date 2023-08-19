package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vadv/gopher-lua-libs/tests"
	"github.com/yuin/gopher-lua"
)

func AssertLuaDatagram(L *lua.LState) int {
	// Check for T:
	var T *testing.T
	ud := L.CheckUserData(1)
	if v, ok := ud.Value.(*testing.T); ok {
		T = v
	} else {
		L.ArgError(1, "testing.T expected")
	}

	dg := CheckDatagram(L, 2)
	dgi := NewDatagramIterator(dg)

	assert.Equal(T, dgi.ReadInt8(), int8(-128))
	assert.Equal(T, dgi.ReadUint8(), uint8(255))
	assert.Equal(T, dgi.ReadInt16(), int16(-32768))
	assert.Equal(T, dgi.ReadUint16(), uint16(65535))
	assert.Equal(T, dgi.ReadInt32(), int32(-2147483648))
	assert.Equal(T, dgi.ReadUint32(), uint32(4294967295))
	assert.Equal(T, dgi.ReadInt64(), int64(-9223372036854775808))
	assert.Equal(T, dgi.ReadBool(), true)
	assert.Equal(T, dgi.ReadString(), "Hello, world!")

	return 1
}

func MakeTestDatagramIterator(L *lua.LState) int {
	dg := NewDatagram()
	dg.AddInt8(-128)
    dg.AddUint8(255)
    dg.AddInt16(-32768)
    dg.AddUint16(65535)
    dg.AddInt32(-2147483648)
    dg.AddUint32(4294967295)
    dg.AddInt64(-9223372036854775808)

    dg.AddBool(true)
    dg.AddString("Hello, world!")

	ud :=NewLuaDatagramIteratorFromDatagram(L, &dg)
	L.Push(ud)

	return 1
}

func PreloadAssertFunctions(L *lua.LState) {
	L.SetGlobal("assertDatagram", L.NewFunction(AssertLuaDatagram))
	L.SetGlobal("makeTestDatagramIterator", L.NewFunction(MakeTestDatagramIterator))
}

func TestWrappers(t *testing.T) {
	preload := tests.SeveralPreloadFuncs(
		RegisterDatagramType,
		RegisterDatagramIteratorType,
		PreloadAssertFunctions,
	)

	assert.NotZero(t, tests.RunLuaTestFile(t, preload, "test/test_wrappers.lua"))
}
