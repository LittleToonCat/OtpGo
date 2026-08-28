package dc

type arrayParameter struct {
	fieldBase

	elementType    packerInterface
	arraySize      int
	arraySizeRange dcUnsignedIntRange
}

func newArrayParameter(elementType packerInterface, size dcUnsignedIntRange) *arrayParameter {
	ap := &arrayParameter{
		fieldBase:      newFieldBase(""),
		elementType:    elementType,
		arraySizeRange: size,
	}

	ap.SetName(elementType.Name())
	elementType.SetName("")

	ap.arraySize = -1
	if size.hasOneValue() {
		ap.arraySize = int(size.getOneValue())
	} else {
		ap.hasRangeLimits = true
	}

	if ap.arraySize >= 0 && elementType.HasFixedByteSize() {
		ap.hasFixedByteSize = true
		ap.fixedByteSize = ap.arraySize * elementType.FixedByteSize()
		ap.hasFixedStructure = true
	} else {

		ap.numLengthBytes = 2
	}

	if elementType.HasRangeLimits() {
		ap.hasRangeLimits = true
	}

	if pb, ok := elementTypeFieldBase(elementType); ok && pb.HasDefaultValue() {
		ap.hasDefaultValue = true
	}

	ap.hasNestedFields = true
	ap.numNestedFields = ap.arraySize
	ap.packType = PTArray

	if sp, ok := elementType.(*simpleParameter); ok {
		if sp.GetType() == STChar {

			ap.packType = PTString
		}
	}

	return ap
}

func elementTypeFieldBase(elementType packerInterface) (*fieldBase, bool) {
	switch t := elementType.(type) {
	case *simpleParameter:
		return &t.fieldBase, true
	case *arrayParameter:
		return &t.fieldBase, true
	case *classParameter:
		return &t.fieldBase, true
	case *switchParameter:
		return &t.fieldBase, true
	default:
		return nil, false
	}
}

func (ap *arrayParameter) GetElementType() packerInterface { return ap.elementType }

func (ap *arrayParameter) AsField() field     { return ap }
func (ap *arrayParameter) AsParameter() field { return ap }

func (ap *arrayParameter) GetDefaultValue() []byte         { return getDefaultValue(&ap.fieldBase, ap) }
func (ap *arrayParameter) ValidateRanges(data []byte) bool { return fieldValidateRanges(ap, data) }
func (ap *arrayParameter) FormatData(data []byte, showFieldNames ...bool) string {
	return fieldFormatData(ap, data, variadicBoolDefaultTrue(showFieldNames))
}
func (ap *arrayParameter) ParseString(s string) []byte { return fieldParseString(ap, s) }

func (ap *arrayParameter) GetArraySize() int { return ap.arraySize }

func (ap *arrayParameter) MakeCopy() packerInterface {
	cp := *ap
	cp.keywordList = *newKeywordList()
	cp.keywordList.CopyKeywords(&ap.keywordList)
	cp.elementType = ap.elementType.MakeCopy()
	return &cp
}

func (ap *arrayParameter) appendArraySpecification(size dcUnsignedIntRange) packerInterface {
	if ap.IsFromTypedef() {

		return newArrayParameter(ap, size)
	}

	ap.elementType = appendArraySpec(ap.elementType, size)
	return ap
}

func appendArraySpec(current packerInterface, size dcUnsignedIntRange) packerInterface {
	if ap, ok := current.(*arrayParameter); ok {
		return ap.appendArraySpecification(size)
	}
	return newArrayParameter(current, size)
}

func (ap *arrayParameter) CalcNumNestedFields(lengthBytes int) int {
	if ap.elementType.HasFixedByteSize() {
		return lengthBytes / ap.elementType.FixedByteSize()
	}
	return -1
}

func (ap *arrayParameter) GetNestedField(int) packerInterface {
	return ap.elementType
}

func (ap *arrayParameter) ValidateNumNestedFields(numNestedFields int) bool {
	rangeError := false
	ap.arraySizeRange.validate(uint(numNestedFields), &rangeError)
	return !rangeError
}

func (ap *arrayParameter) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (ap *arrayParameter) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (ap *arrayParameter) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (ap *arrayParameter) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (ap *arrayParameter) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}

func (ap *arrayParameter) PackString(pd *packData, value string, packError, rangeError *bool) {

	simpleType, ok := ap.elementType.(*simpleParameter)
	if !ok {
		*packError = true
		return
	}

	stringLength := len(value)

	switch simpleType.GetType() {
	case STChar, STUint8, STInt8:
		ap.arraySizeRange.validate(uint(stringLength), rangeError)
		if ap.numLengthBytes != 0 {
			doPackUint16(pd.getWritePointer(2), uint(stringLength))
		}
		pd.appendData([]byte(value))
	default:
		*packError = true
	}
}

func (ap *arrayParameter) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {

	simpleType, ok := ap.elementType.(*simpleParameter)
	if !ok {
		*packError = true
		return
	}

	blobSize := len(value)

	switch simpleType.GetType() {
	case STChar, STUint8, STInt8:
		ap.arraySizeRange.validate(uint(blobSize), rangeError)
		if ap.numLengthBytes != 0 {
			doPackUint16(pd.getWritePointer(2), uint(blobSize))
		}
		pd.appendData(value)
	default:
		*packError = true
	}
}

func (ap *arrayParameter) PackDefaultValue(pd *packData, packError *bool) bool {

	if ap.hasDefaultValue && !ap.defaultValueStale {
		return fieldPackDefaultValue(&ap.fieldBase, pd, packError)
	}

	minimumLength := uint(0)
	if !ap.arraySizeRange.isEmpty() {
		minimumLength = ap.arraySizeRange.getMin(0)
	}

	p := newDCPacker()
	p.beginPack(ap)
	p.push()
	for i := uint(0); i < minimumLength; i++ {
		p.packDefaultValue()
	}
	p.pop()
	if !p.endPack() {
		*packError = true
	} else {
		pd.appendData(p.getData())
	}

	return true
}

func (ap *arrayParameter) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (ap *arrayParameter) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (ap *arrayParameter) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (ap *arrayParameter) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (ap *arrayParameter) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}

func (ap *arrayParameter) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {

	simpleType, ok := ap.elementType.(*simpleParameter)
	if !ok {
		*packError = true
		return
	}

	var stringLength int
	switch simpleType.GetType() {
	case STChar, STUint8, STInt8:
		if ap.numLengthBytes != 0 {
			stringLength = int(doUnpackUint16(data[*p:]))
			*p += 2
		} else {
			stringLength = ap.arraySize
		}
		if *p+stringLength > len(data) {
			*packError = true
			return
		}
		*value = string(data[*p : *p+stringLength])
		*p += stringLength
	default:
		*packError = true
	}
}

func (ap *arrayParameter) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {

	simpleType, ok := ap.elementType.(*simpleParameter)
	if !ok {
		*packError = true
		return
	}

	var blobSize int
	switch simpleType.GetType() {
	case STChar, STUint8, STInt8:
		if ap.numLengthBytes != 0 {
			blobSize = int(doUnpackUint16(data[*p:]))
			*p += 2
		} else {
			blobSize = ap.arraySize
		}
		if *p+blobSize > len(data) {
			*packError = true
			return
		}
		*value = append([]byte(nil), data[*p:*p+blobSize]...)
		*p += blobSize
	default:
		*packError = true
	}
}

func (ap *arrayParameter) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&ap.packerBase, data, p, packError, rangeError)
}
func (ap *arrayParameter) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&ap.packerBase, data, p, packError)
}

func (ap *arrayParameter) GenerateHash(hashgen *hashGenerator) {
	if ap.GetNumKeywords() != 0 {
		ap.keywordList.generateHash(hashgen)
	}
	ap.elementType.GenerateHash(hashgen)
	ap.arraySizeRange.generateHash(hashgen)
}

func (ap *arrayParameter) DoCheckMatch(other packerInterface) bool {
	switch o := other.(type) {
	case *simpleParameter:
		return o.doCheckMatchArrayParameter(ap)
	case *classParameter:
		return o.doCheckMatchArrayParameter(ap)
	case *arrayParameter:
		return ap.doCheckMatchArrayParameter(o)
	default:
		return false
	}
}

func (ap *arrayParameter) doCheckMatchArrayParameter(other *arrayParameter) bool {
	if other == nil {
		return false
	}
	if ap.arraySize != other.arraySize {
		return false
	}
	return ap.elementType.DoCheckMatch(other.elementType)
}
