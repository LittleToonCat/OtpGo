package util

import (
	"otpgo/dc"
	"sync"
)

// Most DCPacker calls isn't thread safe, which would
// lead to problems when multiple operations are happening at once
// under different goroutines.  This mutex must be locked
// when doing any DCPacker related operations and then unlocked
// when done.
var DCLock sync.Mutex = sync.Mutex{}

func DumpVector(data dc.Vector) string {
	dg := NewDatagram()
	dg.AddVector(data)
	return dg.String()
}

func VectorToByte(v dc.Vector) []byte {
	data := []byte{}
	for i := int64(0); i < v.Size(); i++ {
		data = append(data, v.Get(int(i)))
	}
	return data
}

func ByteToVector(data []byte) dc.Vector {
	v := dc.NewVector()
	for _, i := range data {
		v.Add(i)
	}
	return v
}

func ValidateDCRanges(field dc.DCField, data []byte) bool {
	DCLock.Lock()
	defer DCLock.Unlock()

	vector := ByteToVector(data)
	defer dc.DeleteVector(vector)

	return field.ValidateRanges(vector)
}

func FormatFieldData(field dc.DCField, data []byte) string {
	vector := ByteToVector(data)
	defer dc.DeleteVector(vector)

	return field.FormatData(vector)
}

func DumpUnpacker(unpacker dc.DCPacker) string {
	data := []byte(unpacker.GetUnpackString())
	unpackedLength := unpacker.GetNumUnpackedBytes()
	dg := NewDatagram()
	dg.AddData(data)

	dgi := NewDatagramIterator(&dg)
	dgi.Seek(Dgsize_t(unpackedLength))
	return dgi.String()
}
