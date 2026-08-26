package dc

type packerBase struct {
	name              string
	hasFixedByteSize  bool
	fixedByteSize     int
	hasFixedStructure bool
	hasRangeLimits    bool
	numLengthBytes    int
	hasNestedFields   bool
	numNestedFields   int
	packType          DCPackType
}

func newPackerBase(name string) packerBase {
	return packerBase{
		name:            name,
		numNestedFields: -1,
		packType:        PTInvalid,
	}
}

func (b *packerBase) Name() string            { return b.name }
func (b *packerBase) SetName(name string)     { b.name = name }
func (b *packerBase) HasFixedByteSize() bool  { return b.hasFixedByteSize }
func (b *packerBase) FixedByteSize() int      { return b.fixedByteSize }
func (b *packerBase) HasFixedStructure() bool { return b.hasFixedStructure }
func (b *packerBase) HasRangeLimits() bool    { return b.hasRangeLimits }
func (b *packerBase) NumLengthBytes() int     { return b.numLengthBytes }
func (b *packerBase) HasNestedFields() bool   { return b.hasNestedFields }
func (b *packerBase) NumNestedFields() int    { return b.numNestedFields }
func (b *packerBase) PackType() DCPackType    { return b.packType }

func defaultCalcNumNestedFields(lengthBytes int) int { return 0 }

func defaultValidateNumNestedFields(numNestedFields int) bool { return true }

func defaultPackDouble(packError *bool) { *packError = true }
func defaultPackInt(packError *bool)    { *packError = true }
func defaultPackUint(packError *bool)   { *packError = true }
func defaultPackInt64(packError *bool)  { *packError = true }
func defaultPackUint64(packError *bool) { *packError = true }
func defaultPackString(packError *bool) { *packError = true }
func defaultPackBlob(packError *bool)   { *packError = true }
func defaultPackDefaultValue() bool     { return false }

func defaultUnpackDouble(packError *bool) { *packError = true }
func defaultUnpackInt(packError *bool)    { *packError = true }
func defaultUnpackUint(packError *bool)   { *packError = true }
func defaultUnpackInt64(packError *bool)  { *packError = true }
func defaultUnpackUint64(packError *bool) { *packError = true }
func defaultUnpackString(packError *bool) { *packError = true }
func defaultUnpackBlob(packError *bool)   { *packError = true }

func defaultUnpackValidate(b *packerBase, data []byte, p *int, packError, rangeError *bool) bool {
	if !b.hasRangeLimits {
		return defaultUnpackSkip(b, data, p, packError)
	}
	return false
}

func defaultUnpackSkip(b *packerBase, data []byte, p *int, packError *bool) bool {
	if b.hasFixedByteSize {
		*p += b.fixedByteSize
		if *p > len(data) {
			*packError = true
		}
		return true
	}

	if b.hasNestedFields && b.numLengthBytes != 0 {
		if *p+b.numLengthBytes > len(data) {
			*packError = true
		} else {
			var thisLength int
			if b.numLengthBytes == 4 {
				thisLength = int(doUnpackUint32(data[*p : *p+4]))
				*p += thisLength + 4
			} else {
				thisLength = int(doUnpackUint16(data[*p : *p+2]))
				*p += thisLength + 2
			}
			if *p > len(data) {
				*packError = true
			}
		}
		return true
	}

	return false
}
