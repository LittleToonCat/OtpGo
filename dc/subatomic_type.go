package dc

type DCSubatomicType int32

const (
	STInt8 DCSubatomicType = iota
	STInt16
	STInt32
	STInt64

	STUint8
	STUint16
	STUint32
	STUint64

	STFloat64

	STString
	STBlob
	STBlob32
	STInt16array
	STInt32array
	STUint16array
	STUint32array

	STInt8array
	STUint8array

	STUint32uint8array

	STChar

	STInvalid
)

func (t DCSubatomicType) String() string {
	switch t {
	case STInt8:
		return "int8"
	case STInt16:
		return "int16"
	case STInt32:
		return "int32"
	case STInt64:
		return "int64"
	case STUint8:
		return "uint8"
	case STUint16:
		return "uint16"
	case STUint32:
		return "uint32"
	case STUint64:
		return "uint64"
	case STFloat64:
		return "float64"
	case STString:
		return "string"
	case STBlob:
		return "blob"
	case STBlob32:
		return "blob32"
	case STInt8array:
		return "int8array"
	case STInt16array:
		return "int16array"
	case STInt32array:
		return "int32array"
	case STUint8array:
		return "uint8array"
	case STUint16array:
		return "uint16array"
	case STUint32array:
		return "uint32array"
	case STUint32uint8array:
		return "uint32uint8array"
	case STChar:
		return "char"
	case STInvalid:
		return "invalid"
	default:
		return "invalid type"
	}
}

type DCPackType int32

const (
	PTInvalid DCPackType = iota

	PTDouble
	PTInt
	PTUint
	PTInt64
	PTUint64
	PTString
	PTBlob

	PTArray
	PTField
	PTClass
	PTSwitch
)
