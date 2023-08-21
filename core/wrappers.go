package core

import (
	dc "github.com/LittleToonCat/dcparser-go"
	lua "github.com/yuin/gopher-lua"
)

// DC wrappers for Lua

func UnpackDataToLuaValue(unpacker dc.DCPacker, L *lua.LState) lua.LValue {
	var value lua.LValue
	switch unpacker.Get_pack_type() {
	case dc.PT_invalid:
		value = lua.LNil
	case dc.PT_double:
		value = lua.LNumber(unpacker.Unpack_double().(float64))
	case dc.PT_int:
		value = lua.LNumber(unpacker.Unpack_int().(int))
	case dc.PT_uint:
		value = lua.LNumber(unpacker.Unpack_uint().(uint))
	case dc.PT_int64:
		value = lua.LNumber(unpacker.Unpack_int64().(int64))
	case dc.PT_uint64:
		value = lua.LNumber(unpacker.Unpack_uint64().(uint64))
	case dc.PT_string:
		value = lua.LString(unpacker.Unpack_string().(string))
	case dc.PT_blob:
	default:
		// If we reached here, that means it is a list
		// of nested fields (e.g. an array type, an atomic field, a
		// class parameter, or a switch case).
		//
		// We'll have to create a table for these types.
		table := L.NewTable()
		unpacker.Push()
		for unpacker.More_nested_fields() {
			table.Append(UnpackDataToLuaValue(unpacker, L))
		}
		unpacker.Pop()
		value = table
	}

	return value
}

func PackLuaValue(packer dc.DCPacker, value lua.LValue) {
	switch packer.Get_pack_type() {
	case dc.PT_invalid:
	case dc.PT_double:
		fallthrough
	case dc.PT_int:
		fallthrough
	case dc.PT_uint:
		fallthrough
	case dc.PT_int64:
		fallthrough
	case dc.PT_uint64:
		if number, ok := value.(lua.LNumber); ok {
			packer.Pack_double(float64(number))
		}
	case dc.PT_string:
		fallthrough
	case dc.PT_blob:
		if LString, ok := value.(lua.LString); ok {
			packer.Pack_string(string(LString))
		}
	default:
		if table, ok := value.(*lua.LTable); ok {
			packer.Push()
			table.ForEach(func(_, l2 lua.LValue) {
				PackLuaValue(packer, l2)
			})
			packer.Pop()
		}
	}
}
