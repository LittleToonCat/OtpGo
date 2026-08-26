package dc

type atomicField struct {
	fieldBase

	elements []packerInterface
}

func newAtomicField(name string, dclass *dcClass, bogusField bool) *atomicField {
	af := &atomicField{fieldBase: newFieldBaseRaw(name)}
	af.class_ = dclass
	af.bogusField = bogusField
	return af
}

func (af *atomicField) GetNumElements() int { return len(af.elements) }

func (af *atomicField) GetElement(n int) packerInterface { return af.elements[n] }

func (af *atomicField) AsField() field { return af }

func (af *atomicField) AsAtomicField() *atomicField { return af }

func (af *atomicField) GetDefaultValue() Vector         { return getDefaultValue(&af.fieldBase, af) }
func (af *atomicField) ValidateRanges(data Vector) bool { return fieldValidateRanges(af, data) }
func (af *atomicField) FormatData(data Vector, showFieldNames ...bool) string {
	return fieldFormatData(af, data, variadicBoolDefaultTrue(showFieldNames))
}
func (af *atomicField) ParseString(s string) Vector { return fieldParseString(af, s) }

func (af *atomicField) AddElement(element packerInterface) {
	af.elements = append(af.elements, element)
	af.numNestedFields = len(af.elements)

	if af.hasFixedByteSize {
		af.hasFixedByteSize = element.HasFixedByteSize()
		af.fixedByteSize += element.FixedByteSize()
	}
	if af.hasFixedStructure {
		af.hasFixedStructure = element.HasFixedStructure()
	}
	if !af.hasRangeLimits {
		af.hasRangeLimits = element.HasRangeLimits()
	}
	if !af.hasDefaultValue {
		if fb, ok := elementTypeFieldBase(element); ok {
			af.hasDefaultValue = fb.HasDefaultValue()
		}
	}
	af.defaultValueStale = true
}

func (af *atomicField) GetNestedField(n int) packerInterface {
	return af.elements[n]
}

func (af *atomicField) GenerateHash(hashgen *hashGenerator) {
	fieldGenerateHash(&af.fieldBase, hashgen)

	hashgen.addInt(int32(len(af.elements)))
	for _, e := range af.elements {
		e.GenerateHash(hashgen)
	}

	af.keywordList.generateHash(hashgen)
}

func (af *atomicField) CalcNumNestedFields(lengthBytes int) int {
	return defaultCalcNumNestedFields(lengthBytes)
}
func (af *atomicField) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (af *atomicField) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (af *atomicField) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (af *atomicField) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (af *atomicField) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (af *atomicField) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}
func (af *atomicField) PackString(pd *packData, value string, packError, rangeError *bool) {
	defaultPackString(packError)
}
func (af *atomicField) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	defaultPackBlob(packError)
}

func (af *atomicField) PackDefaultValue(pd *packData, packError *bool) bool {
	return fieldPackDefaultValue(&af.fieldBase, pd, packError)
}

func (af *atomicField) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (af *atomicField) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (af *atomicField) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (af *atomicField) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (af *atomicField) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}
func (af *atomicField) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {
	defaultUnpackString(packError)
}
func (af *atomicField) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {
	defaultUnpackBlob(packError)
}
func (af *atomicField) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&af.packerBase, data, p, packError, rangeError)
}
func (af *atomicField) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&af.packerBase, data, p, packError)
}

func (af *atomicField) DoCheckMatch(other packerInterface) bool {
	o, ok := other.(*atomicField)
	if !ok {
		return false
	}
	if len(af.elements) != len(o.elements) {
		return false
	}
	for i := range af.elements {
		if !af.elements[i].DoCheckMatch(o.elements[i]) {
			return false
		}
	}
	return true
}

func (af *atomicField) MakeCopy() packerInterface {
	panic("atomicField.MakeCopy should never be called (atomic fields are not DCParameter subtypes and are never typedef targets)")
}
