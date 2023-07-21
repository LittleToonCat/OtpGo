package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type DatagramIteratorEOF struct {
	err string
}

type DatagramIterator struct {
	Dg     *Datagram
	offset Dgsize_t
	Read   *bytes.Reader
}

func NewDatagramIterator(dg *Datagram) *DatagramIterator {
	dgi := &DatagramIterator{Dg: dg, Read: bytes.NewReader(dg.Bytes())}
	return dgi
}

func (dgi *DatagramIterator) Copy() *DatagramIterator {
	newDgi := NewDatagramIterator(dgi.Dg)
	newDgi.Seek(dgi.Tell())
	return newDgi
}

func (dgi *DatagramIterator) panic(len int8) {
	panic(DatagramIteratorEOF{
		fmt.Sprintf("datagram iterator eof, read length: %d buff length: %d", len, dgi.Read.Len()),
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
	var val int8
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(1)
	}

	dgi.offset += 1
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadInt16() int16 {
	var val int16
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(2)
	}

	dgi.offset += 2
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadInt32() int32 {
	var val int32
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(4)
	}

	dgi.offset += 4
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadInt64() int64 {
	var val int64
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(8)
	}

	dgi.offset += 8
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadUint8() uint8 {
	var val uint8
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(1)
	}

	dgi.offset += 1
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadUint16() uint16 {
	var val uint16
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(2)
	}

	dgi.offset += 2
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadUint32() uint32 {
	var val uint32
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(4)
	}

	dgi.offset += 4
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadUint64() uint64 {
	var val uint64
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(8)
	}

	dgi.offset += 8
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadSize() Dgsize_t {
	var val Dgsize_t
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(Dgsize)
	}

	dgi.offset += Dgsize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadChannel() Channel_t {
	var val Channel_t
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(Chansize)
	}

	dgi.offset += Chansize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadDoid() Doid_t {
	var val Doid_t
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(Doidsize)
	}

	dgi.offset += Doidsize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadZone() Zone_t {
	var val Zone_t
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(Zonesize)
	}

	dgi.offset += Zonesize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadFloat32() float32 {
	var val float32
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(4)
	}

	dgi.offset += 4
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadFloat64() float64 {
	var val float64
	if err := binary.Read(dgi.Read, binary.LittleEndian, &val); err != nil {
		dgi.panic(8)
	}

	dgi.offset += 8
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return val
}

func (dgi *DatagramIterator) ReadString() string {
	sz := dgi.ReadSize()
	buff := make([]byte, sz)
	if _, err := dgi.Read.Read(buff); err != nil {
		dgi.panic(int8(sz))
	}

	dgi.offset += sz
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return string(buff)
}

func (dgi *DatagramIterator) ReadBlob() []uint8 {
	return dgi.ReadData(dgi.ReadSize())
}

func (dgi *DatagramIterator) ReadDatagram() *Datagram {
	data := dgi.ReadBlob()
	dg := NewDatagram()
	dg.Write(data)
	return &dg
}

func (dgi *DatagramIterator) ReadData(length Dgsize_t) []uint8 {
	buff := make([]uint8, int32(length))
	if n, err := dgi.Read.Read(buff); err != nil || n != int(length) {
		dgi.panic(int8(length))
	}

	dgi.offset += length
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	return buff
}

func (dgi *DatagramIterator) ReadRemainder() []uint8 {
	sz := Dgsize_t(dgi.Dg.Len()) - dgi.offset
	return dgi.ReadData(sz)
}

func (dgi *DatagramIterator) RecipientCount() uint8 {
	return dgi.Dg.Bytes()[0]
}

func (dgi *DatagramIterator) Sender() Channel_t {
	offset := dgi.offset

	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount())*Chansize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	sender := dgi.ReadChannel()

	dgi.offset = offset
	return sender
}

func (dgi *DatagramIterator) MessageType() uint16 {
	offset := dgi.offset

	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount()+1)*Chansize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
	msg := dgi.ReadUint16()

	dgi.offset = offset
	return msg
}

func (dgi *DatagramIterator) Tell() Dgsize_t {
	return dgi.offset
}

func (dgi *DatagramIterator) Seek(pos Dgsize_t) {
	dgi.offset = pos
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
}

func (dgi *DatagramIterator) SeekPayload() {
	dgi.offset = 1 + Dgsize_t(dgi.RecipientCount())*Chansize
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
}

func (dgi *DatagramIterator) Skip(len Dgsize_t) {
	if dgi.offset+len > Dgsize_t(dgi.Dg.Len()) {
		dgi.panic(int8(len))
	}

	dgi.offset += len
	dgi.Read.Seek(int64(dgi.offset), io.SeekStart)
}
