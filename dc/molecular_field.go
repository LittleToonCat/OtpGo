package dc

type molecularField struct {
	fieldBase

	fields       []*atomicField
	gotKeywords  bool
	nestedFields []packerInterface
}

func newMolecularField(name string, dclass *dcClass) *molecularField {
	mf := &molecularField{fieldBase: newFieldBaseRaw(name)}
	mf.class_ = dclass
	return mf
}

func (mf *molecularField) GetNumAtomics() int { return len(mf.fields) }

func (mf *molecularField) GetAtomic(n int) *atomicField { return mf.fields[n] }

func (mf *molecularField) AsField() field { return mf }

func (mf *molecularField) AsMolecularField() *molecularField { return mf }

func (mf *molecularField) GetDefaultValue() []byte         { return getDefaultValue(&mf.fieldBase, mf) }
func (mf *molecularField) ValidateRanges(data []byte) bool { return fieldValidateRanges(mf, data) }
func (mf *molecularField) FormatData(data []byte, showFieldNames ...bool) string {
	return fieldFormatData(mf, data, variadicBoolDefaultTrue(showFieldNames))
}
func (mf *molecularField) ParseString(s string) []byte { return fieldParseString(mf, s) }

func (mf *molecularField) AddAtomic(atomic *atomicField) {
	if !atomic.IsBogusField() {
		if !mf.gotKeywords {

			mf.keywordList.CopyKeywords(&atomic.keywordList)
			mf.gotKeywords = true
		}
	}
	mf.fields = append(mf.fields, atomic)

	numAtomicFields := atomic.NumNestedFields()
	for i := 0; i < numAtomicFields; i++ {
		mf.nestedFields = append(mf.nestedFields, atomic.GetNestedField(i))
	}

	mf.numNestedFields = len(mf.nestedFields)

	if mf.hasFixedByteSize {
		mf.hasFixedByteSize = atomic.HasFixedByteSize()
		mf.fixedByteSize += atomic.FixedByteSize()
	}
	if mf.hasFixedStructure {
		mf.hasFixedStructure = atomic.HasFixedStructure()
	}
	if !mf.hasRangeLimits {
		mf.hasRangeLimits = atomic.HasRangeLimits()
	}
	if !mf.hasDefaultValue {
		mf.hasDefaultValue = atomic.HasDefaultValue()
	}
	mf.defaultValueStale = true
}

func (mf *molecularField) GetNestedField(n int) packerInterface {
	return mf.nestedFields[n]
}

func (mf *molecularField) GenerateHash(hashgen *hashGenerator) {
	fieldGenerateHash(&mf.fieldBase, hashgen)

	hashgen.addInt(int32(len(mf.fields)))
	for _, atomic := range mf.fields {
		atomic.GenerateHash(hashgen)
	}
}

func (mf *molecularField) CalcNumNestedFields(lengthBytes int) int {
	return defaultCalcNumNestedFields(lengthBytes)
}
func (mf *molecularField) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (mf *molecularField) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (mf *molecularField) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (mf *molecularField) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (mf *molecularField) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (mf *molecularField) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}
func (mf *molecularField) PackString(pd *packData, value string, packError, rangeError *bool) {
	defaultPackString(packError)
}
func (mf *molecularField) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	defaultPackBlob(packError)
}

func (mf *molecularField) PackDefaultValue(pd *packData, packError *bool) bool {
	return fieldPackDefaultValue(&mf.fieldBase, pd, packError)
}

func (mf *molecularField) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (mf *molecularField) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (mf *molecularField) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (mf *molecularField) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (mf *molecularField) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}
func (mf *molecularField) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {
	defaultUnpackString(packError)
}
func (mf *molecularField) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {
	defaultUnpackBlob(packError)
}
func (mf *molecularField) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&mf.packerBase, data, p, packError, rangeError)
}
func (mf *molecularField) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&mf.packerBase, data, p, packError)
}

func (mf *molecularField) DoCheckMatch(other packerInterface) bool {
	o, ok := other.(*molecularField)
	if !ok {
		return false
	}
	if len(mf.nestedFields) != len(o.nestedFields) {
		return false
	}
	for i := range mf.nestedFields {
		if !mf.nestedFields[i].DoCheckMatch(o.nestedFields[i]) {
			return false
		}
	}
	return true
}

func (mf *molecularField) MakeCopy() packerInterface {
	panic("molecularField.MakeCopy should never be called (molecular fields are not DCParameter subtypes and are never typedef targets)")
}
