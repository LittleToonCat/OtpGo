package dc

type classParameter struct {
	fieldBase

	nestedFields []packerInterface
	dclass       *dcClass
}

func newClassParameter(dclass *dcClass) *classParameter {
	cp := &classParameter{fieldBase: newFieldBase(dclass.GetName()), dclass: dclass}

	numFields := dclass.GetNumInheritedFields()

	cp.hasNestedFields = true
	cp.packType = PTClass

	if dclass.HasConstructor() {
		f := dclass.GetConstructor()
		cp.nestedFields = append(cp.nestedFields, f)
		cp.hasDefaultValue = cp.hasDefaultValue || f.HasDefaultValue()
	}
	for i := 0; i < numFields; i++ {
		f := dclass.GetInheritedField(i)
		if _, isMolecular := f.(*molecularField); !isMolecular {
			cp.nestedFields = append(cp.nestedFields, f)
			cp.hasDefaultValue = cp.hasDefaultValue || f.HasDefaultValue()
		}
	}
	cp.numNestedFields = len(cp.nestedFields)

	cp.hasFixedByteSize = true
	cp.fixedByteSize = 0
	cp.hasFixedStructure = true
	for i := 0; i < cp.numNestedFields; i++ {
		f := cp.GetNestedField(i)
		cp.hasFixedByteSize = cp.hasFixedByteSize && f.HasFixedByteSize()
		cp.fixedByteSize += f.FixedByteSize()
		cp.hasFixedStructure = cp.hasFixedStructure && f.HasFixedStructure()
		cp.hasRangeLimits = cp.hasRangeLimits || f.HasRangeLimits()
	}

	return cp
}

func (cp *classParameter) IsValid() bool { return !cp.dclass.IsBogusClass() }

func (cp *classParameter) AsField() field     { return cp }
func (cp *classParameter) AsParameter() field { return cp }

func (cp *classParameter) GetDefaultValue() Vector         { return getDefaultValue(&cp.fieldBase, cp) }
func (cp *classParameter) ValidateRanges(data Vector) bool { return fieldValidateRanges(cp, data) }
func (cp *classParameter) FormatData(data Vector, showFieldNames ...bool) string {
	return fieldFormatData(cp, data, variadicBoolDefaultTrue(showFieldNames))
}
func (cp *classParameter) ParseString(s string) Vector { return fieldParseString(cp, s) }

func (cp *classParameter) GetWrappedClass() *dcClass { return cp.dclass }

func (cp *classParameter) GetNestedField(n int) packerInterface {
	return cp.nestedFields[n]
}

func (cp *classParameter) CalcNumNestedFields(lengthBytes int) int {
	return defaultCalcNumNestedFields(lengthBytes)
}
func (cp *classParameter) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (cp *classParameter) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (cp *classParameter) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (cp *classParameter) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (cp *classParameter) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (cp *classParameter) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}
func (cp *classParameter) PackString(pd *packData, value string, packError, rangeError *bool) {
	defaultPackString(packError)
}
func (cp *classParameter) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	defaultPackBlob(packError)
}

func (cp *classParameter) PackDefaultValue(pd *packData, packError *bool) bool {
	return fieldPackDefaultValue(&cp.fieldBase, pd, packError)
}

func (cp *classParameter) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (cp *classParameter) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (cp *classParameter) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (cp *classParameter) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (cp *classParameter) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}
func (cp *classParameter) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {
	defaultUnpackString(packError)
}
func (cp *classParameter) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {
	defaultUnpackBlob(packError)
}
func (cp *classParameter) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&cp.packerBase, data, p, packError, rangeError)
}
func (cp *classParameter) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&cp.packerBase, data, p, packError)
}

func (cp *classParameter) GenerateHash(hashgen *hashGenerator) {
	if cp.GetNumKeywords() != 0 {
		cp.keywordList.generateHash(hashgen)
	}
	cp.dclass.GenerateHash(hashgen)
}

func (cp *classParameter) DoCheckMatch(other packerInterface) bool {
	switch o := other.(type) {
	case *classParameter:
		return cp.doCheckMatchClassParameter(o)
	case *arrayParameter:
		return cp.doCheckMatchArrayParameter(o)
	default:
		return false
	}
}

func (cp *classParameter) doCheckMatchClassParameter(other *classParameter) bool {
	if len(cp.nestedFields) != len(other.nestedFields) {
		return false
	}
	for i := range cp.nestedFields {
		if !cp.nestedFields[i].DoCheckMatch(other.nestedFields[i]) {
			return false
		}
	}
	return true
}

func (cp *classParameter) doCheckMatchArrayParameter(other *arrayParameter) bool {
	if len(cp.nestedFields) != other.arraySize {
		return false
	}
	elementType := other.elementType
	for _, f := range cp.nestedFields {
		if !f.DoCheckMatch(elementType) {
			return false
		}
	}
	return true
}

func (cp *classParameter) MakeCopy() packerInterface {
	newCP := *cp
	newCP.keywordList = *newKeywordList()
	newCP.keywordList.CopyKeywords(&cp.keywordList)
	newCP.nestedFields = append([]packerInterface(nil), cp.nestedFields...)
	return &newCP
}
