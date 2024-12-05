package util

import (
	"otpgo/dc"
	"sync"
)

var DCLock sync.Mutex = sync.Mutex{}

func DumpVector(data dc.Vector_uchar) string {
	dg := NewDatagram()
	dg.AddVector(data)
	return dg.String()
}

func VectorToByte(v dc.Vector_uchar) []byte {
	data := []byte{}
	for i := int64(0); i < v.Size(); i++ {
		data = append(data, v.Get(int(i)))
	}
	return data
}

func ByteToVector(data []byte) dc.Vector_uchar {
	v := dc.NewVector_uchar()
	for _, i := range data {
		v.Add(i)
	}
	return v
}

func ValidateDCRanges(field dc.DCField, data []byte) bool {
	DCLock.Lock()
	defer DCLock.Unlock()

	vector := ByteToVector(data)
	defer dc.DeleteVector_uchar(vector)

	return field.Validate_ranges(vector)
}

func FormatFieldData(field dc.DCField, data []byte) string {
	vector := ByteToVector(data)
	defer dc.DeleteVector_uchar(vector)

	return field.Format_data(vector)
}

func DumpUnpacker(unpacker dc.DCPacker) string {
	data := []byte(unpacker.Get_unpack_string())
	unpackedLength := unpacker.Get_num_unpacked_bytes()
	dg := NewDatagram()
	dg.AddData(data)

	dgi := NewDatagramIterator(&dg)
	dgi.Seek(Dgsize_t(unpackedLength))
	return dgi.String()
}
