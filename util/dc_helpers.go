package util

import (
	"otpgo/dc"
)

func ValidateDCRanges(field dc.DCField, data []byte) bool {
	return field.ValidateRanges(data)
}

func DumpBytes(data []byte) string {
	dg := NewDatagram()
	dg.AddData(data)
	return dg.String()
}

func FormatFieldData(field dc.DCField, data []byte) string {
	return field.FormatData(data)
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
