package dc

import "math"

func doPackInt8(buffer []byte, value int) {
	buffer[0] = byte(value & 0xff)
}

func doPackInt16(buffer []byte, value int) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
}

func doPackInt32(buffer []byte, value int) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
	buffer[2] = byte((value >> 16) & 0xff)
	buffer[3] = byte((value >> 24) & 0xff)
}

func doPackInt64(buffer []byte, value int64) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
	buffer[2] = byte((value >> 16) & 0xff)
	buffer[3] = byte((value >> 24) & 0xff)
	buffer[4] = byte((value >> 32) & 0xff)
	buffer[5] = byte((value >> 40) & 0xff)
	buffer[6] = byte((value >> 48) & 0xff)
	buffer[7] = byte((value >> 56) & 0xff)
}

func doPackUint8(buffer []byte, value uint) {
	buffer[0] = byte(value & 0xff)
}

func doPackUint16(buffer []byte, value uint) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
}

func doPackUint32(buffer []byte, value uint) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
	buffer[2] = byte((value >> 16) & 0xff)
	buffer[3] = byte((value >> 24) & 0xff)
}

func doPackUint64(buffer []byte, value uint64) {
	buffer[0] = byte(value & 0xff)
	buffer[1] = byte((value >> 8) & 0xff)
	buffer[2] = byte((value >> 16) & 0xff)
	buffer[3] = byte((value >> 24) & 0xff)
	buffer[4] = byte((value >> 32) & 0xff)
	buffer[5] = byte((value >> 40) & 0xff)
	buffer[6] = byte((value >> 48) & 0xff)
	buffer[7] = byte((value >> 56) & 0xff)
}

func doPackFloat64(buffer []byte, value float64) {
	bits := math.Float64bits(value)
	doPackUint64(buffer, bits)
}

func doUnpackInt8(buffer []byte) int {
	return int(int8(buffer[0]))
}

func doUnpackInt16(buffer []byte) int {
	return int(int16(uint16(buffer[0]) | uint16(int8(buffer[1]))<<8))
}

func doUnpackInt32(buffer []byte) int {
	return int(int32(uint32(buffer[0]) |
		uint32(buffer[1])<<8 |
		uint32(buffer[2])<<16 |
		uint32(int8(buffer[3]))<<24))
}

func doUnpackInt64(buffer []byte) int64 {
	return int64(uint64(buffer[0]) |
		uint64(buffer[1])<<8 |
		uint64(buffer[2])<<16 |
		uint64(buffer[3])<<24 |
		uint64(buffer[4])<<32 |
		uint64(buffer[5])<<40 |
		uint64(buffer[6])<<48 |
		uint64(int8(buffer[7]))<<56)
}

func doUnpackUint8(buffer []byte) uint {
	return uint(buffer[0])
}

func doUnpackUint16(buffer []byte) uint {
	return uint(buffer[0]) | uint(buffer[1])<<8
}

func doUnpackUint32(buffer []byte) uint {
	return uint(buffer[0]) | uint(buffer[1])<<8 | uint(buffer[2])<<16 | uint(buffer[3])<<24
}

func doUnpackUint64(buffer []byte) uint64 {
	return uint64(buffer[0]) | uint64(buffer[1])<<8 | uint64(buffer[2])<<16 | uint64(buffer[3])<<24 |
		uint64(buffer[4])<<32 | uint64(buffer[5])<<40 | uint64(buffer[6])<<48 | uint64(buffer[7])<<56
}

func doUnpackFloat64(buffer []byte) float64 {
	return math.Float64frombits(doUnpackUint64(buffer))
}

func validateIntLimits(value int, numBits int, rangeError *bool) {

	mask := (1 << (numBits - 1)) - 1
	value |= mask

	if value != mask && value != -1 {
		*rangeError = true
	}
}

func validateInt64Limits(value int64, numBits int, rangeError *bool) {
	mask := (int64(1) << (numBits - 1)) - 1
	value |= mask
	if value != mask && value != -1 {
		*rangeError = true
	}
}

func validateUintLimits(value uint, numBits int, rangeError *bool) {

	mask := (uint(1) << numBits) - 1
	value &^= mask
	if value != 0 {
		*rangeError = true
	}
}

func validateUint64Limits(value uint64, numBits int, rangeError *bool) {
	mask := (uint64(1) << numBits) - 1
	value &^= mask
	if value != 0 {
		*rangeError = true
	}
}
