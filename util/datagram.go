package util

import (
	"bytes"
	"encoding/binary"
)

type Datagram struct {
	*bytes.Buffer
}

func NewDatagram() Datagram {
	var b bytes.Buffer
	return Datagram{&b}
}

//  Bufio will automatically take care of type sizes for us. In these cases, we're not
//  going to handle a panic created by binary. Write as an unsuccessful write to a buffer would
//  indicate a fatal error, anyways.
func (d *Datagram) AddInt8(v int8)          { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddUint8(v uint8)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddInt16(v int16)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddUint16(v uint16)      { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddInt32(v int32)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddUint32(v uint32)      { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddInt64(v int64)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddUint64(v uint64)      { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddFloat32(v float32)    { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddFloat64(v float64)    { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddSize(v Dgsize_t)      { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddChannel(v Channel_t)  { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddDoid(v Doid_t)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddZone(v Zone_t)        { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddBool(v bool)          { binary.Write(d, binary.LittleEndian, v) }
func (d *Datagram) AddData(v []byte)        { d.Write(v) }
func (d *Datagram) AddDatagram(v *Datagram) { d.Write(v.Bytes()) }
func (d *Datagram) AddString(v string) {
	d.AddSize(Dgsize_t(len(v)))
	d.Write([]byte(v))
}

func (d *Datagram) AddLocation(parent Doid_t, zone Zone_t) {
	binary.Write(d, binary.LittleEndian, parent)
	binary.Write(d, binary.LittleEndian, zone)
}

func (d *Datagram) AddDataBlob(v []byte) {
	d.AddSize(Dgsize_t(len(v)))
	d.Write(v)
}

func (d *Datagram) AddBlob(v *Datagram) {
	d.AddSize(Dgsize_t(v.Len()))
	d.Write(v.Bytes())
}

func (d *Datagram) AddServerHeader(to Channel_t, from Channel_t, messageType uint16) {
	d.AddUint8(1)
	d.AddChannel(to)
	d.AddChannel(from)
	d.AddUint16(messageType)
}

func (d *Datagram) AddMultipleServerHeader(recipients []Channel_t, from Channel_t, messageType uint16) {
	d.AddUint8(uint8(len(recipients)))
	for _, channel := range recipients {
		d.AddChannel(channel)
	}
	d.AddChannel(from)
	d.AddUint16(messageType)
}

func (d *Datagram) AddControlHeader(messageType uint16) {
	d.AddUint8(1)
	d.AddChannel(CONTROL_MESSAGE)
	d.AddUint16(messageType)
}
