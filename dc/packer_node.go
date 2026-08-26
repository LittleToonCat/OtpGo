package dc

type packerInterface interface {
	Name() string
	SetName(name string)
	HasFixedByteSize() bool
	FixedByteSize() int
	HasFixedStructure() bool
	HasRangeLimits() bool
	NumLengthBytes() int
	HasNestedFields() bool
	NumNestedFields() int
	PackType() DCPackType

	CalcNumNestedFields(lengthBytes int) int
	GetNestedField(n int) packerInterface
	ValidateNumNestedFields(numNestedFields int) bool

	PackDouble(pd *packData, value float64, packError, rangeError *bool)
	PackInt(pd *packData, value int, packError, rangeError *bool)
	PackUint(pd *packData, value uint, packError, rangeError *bool)
	PackInt64(pd *packData, value int64, packError, rangeError *bool)
	PackUint64(pd *packData, value uint64, packError, rangeError *bool)
	PackString(pd *packData, value string, packError, rangeError *bool)
	PackBlob(pd *packData, value []byte, packError, rangeError *bool)

	PackDefaultValue(pd *packData, packError *bool) bool

	UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool)
	UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool)
	UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool)
	UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool)
	UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool)
	UnpackString(data []byte, p *int, value *string, packError, rangeError *bool)
	UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool)

	UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool
	UnpackSkip(data []byte, p *int, packError *bool) bool

	GenerateHash(hashgen *hashGenerator)

	DoCheckMatch(other packerInterface) bool

	MakeCopy() packerInterface
}
