package util

import (
	"encoding/binary"
	"fmt"
	"math"
	"otpgo/dc"
)

type DatagramIteratorEOF struct {
	err string
}

type DatagramIterator struct {
	Dg     *Datagram
	offset Dgsize_t
}

func NewDatagramIterator(dg *Datagram) *DatagramIterator {
	dgi := &DatagramIterator{Dg: dg}
	return dgi
}

// take advances the iterator by n bytes and returns them.
func (dgi *DatagramIterator) take(n Dgsize_t) []byte {
	buf := dgi.Dg.Bytes()
	sz := Dgsize_t(len(buf))
	if dgi.offset > sz || n > sz-dgi.offset {
		dgi.panic(int8(min(n, math.MaxInt8)))
	}

	start := dgi.offset
	dgi.offset += n
	return buf[start:dgi.offset]
}

func (dgi *DatagramIterator) String() string {
	return fmt.Sprintf(
		"DatagramIterator:\n"+
			"offset: %d (of %d) / 0x%x (of 0x%x)\n"+
			"%s",
		dgi.offset, dgi.Dg.Len(), dgi.offset, dgi.Dg.Len(),
		dgi.Dg,
	)
}

func (dgi *DatagramIterator) Copy() *DatagramIterator {
	newDgi := NewDatagramIterator(dgi.Dg)
	newDgi.Seek(dgi.Tell())
	return newDgi
}

func (dgi *DatagramIterator) panic(len int8) {
	panic(DatagramIteratorEOF{
		fmt.Sprintf("datagram iterator eof, read length: %d buff length: %d", len, dgi.RemainingSize()),
	})
}

func (dgi *DatagramIterator) ReadBool() bool {
	val := dgi.ReadUint8()
	if val != 0 {
		return true
	} else {
		return false
	}
}

func (dgi *DatagramIterator) ReadInt8() int8 {
	return int8(dgi.take(1)[0])
}

func (dgi *DatagramIterator) ReadInt16() int16 {
	return int16(binary.LittleEndian.Uint16(dgi.take(2)))
}

func (dgi *DatagramIterator) ReadInt32() int32 {
	return int32(binary.LittleEndian.Uint32(dgi.take(4)))
}

func (dgi *DatagramIterator) ReadInt64() int64 {
	return int64(binary.LittleEndian.Uint64(dgi.take(8)))
}

func (dgi *DatagramIterator) ReadUint8() uint8 {
	return dgi.take(1)[0]
}

func (dgi *DatagramIterator) ReadUint16() uint16 {
	return binary.LittleEndian.Uint16(dgi.take(2))
}

func (dgi *DatagramIterator) ReadUint32() uint32 {
	return binary.LittleEndian.Uint32(dgi.take(4))
}

func (dgi *DatagramIterator) ReadUint64() uint64 {
	return binary.LittleEndian.Uint64(dgi.take(8))
}

func (dgi *DatagramIterator) ReadSize() Dgsize_t {
	return Dgsize_t(binary.LittleEndian.Uint32(dgi.take(Dgsize)))
}

func (dgi *DatagramIterator) ReadChannel() Channel_t {
	return Channel_t(binary.LittleEndian.Uint64(dgi.take(Chansize)))
}

func (dgi *DatagramIterator) ReadDoid() Doid_t {
	return Doid_t(binary.LittleEndian.Uint32(dgi.take(Doidsize)))
}

func (dgi *DatagramIterator) ReadZone() Zone_t {
	return Zone_t(binary.LittleEndian.Uint32(dgi.take(Zonesize)))
}

func (dgi *DatagramIterator) ReadFloat32() float32 {
	return math.Float32frombits(binary.LittleEndian.Uint32(dgi.take(4)))
}

func (dgi *DatagramIterator) ReadFloat64() float64 {
	return math.Float64frombits(binary.LittleEndian.Uint64(dgi.take(8)))
}

func (dgi *DatagramIterator) ReadString() string {
	sz := dgi.ReadUint16()
	if sz == 0 {
		return ""
	}
	return string(dgi.take(Dgsize_t(sz)))
}

func (dgi *DatagramIterator) ReadString32() string {
	sz := dgi.ReadSize()
	if sz == 0 {
		return ""
	}
	return string(dgi.take(sz))
}

func (dgi *DatagramIterator) ReadBlob() []uint8 {
	return dgi.ReadData(Dgsize_t(dgi.ReadUint16()))
}

func (dgi *DatagramIterator) ReadVector() dc.Vector {
	return dc.Vector(dgi.ReadBlob())
}

func (dgi *DatagramIterator) ReadBlob32() []uint8 {
	return dgi.ReadData(dgi.ReadSize())
}

func (dgi *DatagramIterator) ReadDatagram() *Datagram {
	data := dgi.ReadBlob()
	dg := NewDatagram()
	dg.Write(data)
	return &dg
}

func (dgi *DatagramIterator) ReadData(length Dgsize_t) []uint8 {
	if length == 0 {
		return []uint8{}
	}

	src := dgi.take(length)

	buff := make([]uint8, length)
	copy(buff, src)
	return buff
}

func (dgi *DatagramIterator) ReadRemainder() []uint8 {
	sz := Dgsize_t(dgi.Dg.Len()) - dgi.offset
	return dgi.ReadData(sz)
}

func (dgi *DatagramIterator) ReadRemainderAsVector() dc.Vector {
	return dc.Vector(dgi.ReadRemainder())
}

func (dgi *DatagramIterator) ReadDCField(field dc.DCField, validateRanges bool, lock bool) ([]byte, bool) {
	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	offset := dgi.Tell()

	vectorData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector(vectorData)

	dgi.Seek(offset)

	unpacker.SetUnpackData(vectorData)
	unpacker.BeginUnpack(field)

	packedData := unpacker.UnpackLiteralValue()
	defer dc.DeleteVector(packedData)

	if !unpacker.EndUnpack() {
		return nil, false
	}

	if validateRanges && !field.ValidateRanges(packedData) {
		return nil, false
	}

	dgi.Seek(offset + Dgsize_t(unpacker.GetNumUnpackedBytes()))
	return []byte(packedData), true
}


func (dgi *DatagramIterator) SkipDCField(field dc.DCField, lock bool) bool {
	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	offset := dgi.Tell()

	// We need data to skip, or else it'll assert an error.
	vectorData := dgi.ReadRemainderAsVector()
	defer dc.DeleteVector(vectorData)

	dgi.Seek(offset)

	unpacker.SetUnpackData(vectorData)
	unpacker.BeginUnpack(field)
	unpacker.UnpackSkip()
	if !unpacker.EndUnpack() {
		return false
	}

	dgi.Seek(offset + Dgsize_t(unpacker.GetNumUnpackedBytes()))
	return true
}

func (dgi *DatagramIterator) RecipientCount() uint8 {
	return dgi.Dg.Bytes()[0]
}

func (dgi *DatagramIterator) Sender() Channel_t {
	offset := dgi.offset

	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount())*Chansize
	sender := dgi.ReadChannel()

	dgi.offset = offset
	return sender
}

func (dgi *DatagramIterator) MessageType() uint16 {
	offset := dgi.offset

	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount()+1)*Chansize
	msg := dgi.ReadUint16()

	dgi.offset = offset
	return msg
}

func (dgi *DatagramIterator) Tell() Dgsize_t {
	return dgi.offset
}

func (dgi *DatagramIterator) Seek(pos Dgsize_t) {
	dgi.offset = pos
}

func (dgi *DatagramIterator) SeekPayload() {
	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount())*Chansize
}

func (dgi *DatagramIterator) Skip(len Dgsize_t) {
	dgi.take(len)
}

func (dgi *DatagramIterator) RemainingSize() Dgsize_t {
	return Dgsize_t(dgi.Dg.Len()) - dgi.offset
}
