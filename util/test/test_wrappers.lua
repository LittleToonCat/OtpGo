function TestDatagram(t)
    -- Create a new Datagram
    local dg = datagram:new()

    -- Add some data.
    dg:addInt8(-128)
    dg:addUint8(255)
    dg:addInt16(-32768)
    dg:addUint16(65535)
    dg:addInt32(-2147483648)
    dg:addUint32(4294967295)
    dg:addInt64(-9223372036854775808)

    dg:addBool(true)
    dg:addString("Hello, world!")

    -- Pass it over to Go and make sure that the values are the same.
    assertDatagram(t, dg)
end

function TestDatagramIteratorFromGo(t)
    -- Test Go passing over the Iterator to Lua.
    local dgi = makeTestDatagramIterator()

    -- Verify data:
    assert(dgi:readInt8() == -128, "dgi:readInt8() ~= -128")
    assert(dgi:readUint8() == 255, "dgi:readUint8() ~= 255")
    assert(dgi:readInt16() == -32768, "dgi:readInt16() ~= -32768")
    assert(dgi:readUint16() == 65535, "dgi:readUint16() ~= 65535")
    assert(dgi:readInt32() == -2147483648, "dgi:readInt32() ~= -2147483648")
    assert(dgi:readUint32() == 4294967295, "dgi:readUint32() ~= 4294967295")
    assert(dgi:readInt64() == -9223372036854775808, "dgi:readInt64() ~= -9223372036854775808")
    assert(dgi:readBool() == true, "dgi:readBool() ~= true")
    assert(dgi:readString() == "Hello, world!", "dgi:readString() ~= \"Hello, world!\"")
end
