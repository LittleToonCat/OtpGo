---@meta

---@class datagram
datagram = {}

--- Creates a new Datagram
---@return datagram
function datagram:new() end

---Adds the server header containing the channels to send, the from channel, and a message type.
---@param to number | number[]
---@param from number
---@param messageType number
function datagram:addServerHeader(to, from, messageType) end

---Adds a signed 8-bit interger to the datagram
---@param value number
function datagram:addInt8(value) end

---Adds an unsigned 8-bit interger to a datagram
---@param value number
function datagram:addUint8(value) end

---Adds a signed 16-bit interger to the datagram
---@param value number
function datagram:addInt16(value) end

---Adds an unsigned 16-bit interger to a datagram
---@param value number
function datagram:addUint16(value) end

---Adds a signed 32-bit interger to the datagram
---@param value number
function datagram:addInt32(value) end

---Adds an unsigned 32-bit interger to a datagram
---@param value number
function datagram:addUint32(value) end

---Adds a signed 64-bit interger to the datagram
---@param value int64 | number
function datagram:addInt64(value) end

---Adds an unsigned 64-bit interger to a datagram
---@param value uint64 | number
function datagram:addUint64(value) end

---Adds a boolean to a datagram
---@param boolean boolean
function datagram:addBool(boolean) end

---Adds a string (with a 16-bit header) to a datagram
---@param string string
function datagram:addString(string) end

---Adds a data string (without a 16-bit header) to a datagram
---@param data string
function datagram:addData(data) end


---@class dgi
dgi = {}

---Creates a new Datagram Iterator.
---@param dg datagram
---@return dgi
function dgi:new(dg) end

---Gets the remaining size of a datagram
---@return number
function dgi:getRemainingSize() end

---Reads and returns a 8-bit signed interger from a datagram.
---@return number
function dgi:readInt8() end

---Reads and returns a 8-bit unsigned interger from a datagram.
---@return number
function dgi:readUint8() end

---Reads and returns a 16-bit signed interger from a datagram.
---@return number
function dgi:readInt16() end

---Reads and returns a 16-bit unsigned interger from a datagram.
---@return number
function dgi:readUint16() end

---Reads and returns a 32-bit signed interger from a datagram.
---@return number
function dgi:readInt32() end

---Reads and returns a 32-bit unsigned interger from a datagram.
---@return number
function dgi:readUint32() end

---Reads and returns a 64-bit signed interger from a datagram.
---@return int64
function dgi:readInt64() end

---Reads and returns a 64-bit unsigned interger from a datagram.
---@return uint64
function dgi:readUint64() end

---Reads and return a boolean from a datagram.
---@return boolean
function dgi:readBool() end

---Reads and returns a string from a datagram.
---@return string
function dgi:readString() end

---Reads the remainder of the datagram and returns it as a string
---@return string
function dgi:readRemainder() end

---Reads the set size of bytes from the datagram and returns it as a string.
---@param size number
---@return string
function dgi:readFixedString(size) end
