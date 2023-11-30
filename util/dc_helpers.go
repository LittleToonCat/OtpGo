package util

import (
	dc "github.com/LittleToonCat/dcparser-go"
)

func DumpVector(data dc.Vector_uchar) string {
	dg := NewDatagram()
	dg.AddVector(data)
	return dg.String()
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
