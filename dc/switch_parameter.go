package dc

type switchParameter struct {
	fieldBase

	dswitch *dcSwitch
}

func newSwitchParameter(dswitch *dcSwitch) *switchParameter {
	sw := &switchParameter{fieldBase: newFieldBase(dswitch.GetName()), dswitch: dswitch}

	sw.hasFixedByteSize = true
	sw.fixedByteSize = 0
	sw.hasFixedStructure = false

	sw.hasNestedFields = true
	sw.numNestedFields = 1
	sw.packType = PTSwitch

	keyParameter := dswitch.GetKeyParameter()
	sw.hasFixedByteSize = sw.hasFixedByteSize && keyParameter.HasFixedByteSize()
	sw.hasRangeLimits = sw.hasRangeLimits || keyParameter.HasRangeLimits()
	sw.hasDefaultValue = sw.hasDefaultValue || keyParameter.HasDefaultValue()

	numCases := dswitch.GetNumCases()
	if numCases > 0 {
		sw.fixedByteSize = dswitch.GetCase(0).FixedByteSize()

		for i := 0; i < numCases; i++ {
			fields := dswitch.GetCase(i)
			if !fields.HasFixedByteSize() || fields.FixedByteSize() != sw.fixedByteSize {
				sw.hasFixedByteSize = false
			}
			sw.hasRangeLimits = sw.hasRangeLimits || fields.HasRangeLimits()
			sw.hasDefaultValue = sw.hasDefaultValue || fields.hasDefaultValueFlag
		}
	}

	if fields := dswitch.GetDefaultCase(); fields != nil {
		if !fields.HasFixedByteSize() || fields.FixedByteSize() != sw.fixedByteSize {
			sw.hasFixedByteSize = false
		}
		sw.hasRangeLimits = sw.hasRangeLimits || fields.HasRangeLimits()
		sw.hasDefaultValue = sw.hasDefaultValue || fields.hasDefaultValueFlag
	}

	return sw
}

func (sw *switchParameter) IsValid() bool { return true }

func (sw *switchParameter) AsField() field     { return sw }
func (sw *switchParameter) AsParameter() field { return sw }

func (sw *switchParameter) GetDefaultValue() Vector         { return getDefaultValue(&sw.fieldBase, sw) }
func (sw *switchParameter) ValidateRanges(data Vector) bool { return fieldValidateRanges(sw, data) }
func (sw *switchParameter) FormatData(data Vector, showFieldNames ...bool) string {
	return fieldFormatData(sw, data, variadicBoolDefaultTrue(showFieldNames))
}
func (sw *switchParameter) ParseString(s string) Vector { return fieldParseString(sw, s) }
func (sw *switchParameter) GetSwitch() *dcSwitch        { return sw.dswitch }

func (sw *switchParameter) GetNestedField(int) packerInterface {
	return sw.dswitch.GetKeyParameter()
}

func (sw *switchParameter) applySwitch(valueData []byte) packerInterface {
	fields := sw.dswitch.applySwitch(valueData)
	if fields == nil {
		return nil
	}
	return fields
}

func (sw *switchParameter) CalcNumNestedFields(lengthBytes int) int {
	return defaultCalcNumNestedFields(lengthBytes)
}
func (sw *switchParameter) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (sw *switchParameter) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (sw *switchParameter) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (sw *switchParameter) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (sw *switchParameter) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (sw *switchParameter) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}
func (sw *switchParameter) PackString(pd *packData, value string, packError, rangeError *bool) {
	defaultPackString(packError)
}
func (sw *switchParameter) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	defaultPackBlob(packError)
}

func (sw *switchParameter) PackDefaultValue(pd *packData, packError *bool) bool {
	if sw.HasDefaultValue() {
		return fieldPackDefaultValue(&sw.fieldBase, pd, packError)
	}
	return sw.dswitch.PackDefaultValue(pd, packError)
}

func (sw *switchParameter) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (sw *switchParameter) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (sw *switchParameter) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (sw *switchParameter) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (sw *switchParameter) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}
func (sw *switchParameter) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {
	defaultUnpackString(packError)
}
func (sw *switchParameter) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {
	defaultUnpackBlob(packError)
}
func (sw *switchParameter) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&sw.packerBase, data, p, packError, rangeError)
}
func (sw *switchParameter) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&sw.packerBase, data, p, packError)
}

func (sw *switchParameter) GenerateHash(hashgen *hashGenerator) {
	if sw.GetNumKeywords() != 0 {
		sw.keywordList.generateHash(hashgen)
	}
	sw.dswitch.GenerateHash(hashgen)
}

func (sw *switchParameter) DoCheckMatch(other packerInterface) bool {
	o, ok := other.(*switchParameter)
	if !ok {
		return false
	}
	return sw.dswitch.DoCheckMatchSwitch(o.dswitch)
}

func (sw *switchParameter) MakeCopy() packerInterface {
	newSW := *sw
	newSW.keywordList = *newKeywordList()
	newSW.keywordList.CopyKeywords(&sw.keywordList)
	return &newSW
}
