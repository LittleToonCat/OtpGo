package util

import (
	"otpgo/dc"
)

func DumpBytes(data []byte) string {
	dg := NewDatagram()
	dg.AddData(data)
	return dg.String()
}

func DumpUnpacker(unpacker *dc.DCPacker) string {
	data := []byte(unpacker.GetUnpackString())
	unpackedLength := unpacker.GetNumUnpackedBytes()
	dg := NewDatagram()
	dg.AddData(data)

	dgi := NewDatagramIterator(&dg)
	dgi.Seek(Dgsize_t(unpackedLength))
	return dgi.String()
}
