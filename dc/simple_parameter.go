package dc

import "math"

type simpleParameter struct {
	fieldBase

	subatomicType DCSubatomicType
	divisor       uint32

	nestedType      DCSubatomicType
	nestedField     packerInterface
	bytesPerElement int

	origRange   dcDoubleRange
	hasModulus  bool
	origModulus float64

	intRange    dcIntRange
	uintRange   dcUnsignedIntRange
	int64Range  dcInt64Range
	uint64Range dcUnsignedInt64Range
	doubleRange dcDoubleRange

	uintModulus   uint32
	uint64Modulus uint64
	doubleModulus float64
}

var nestedFieldMap = map[DCSubatomicType]map[uint32]*simpleParameter{}

func newSimpleParameter(subatomicType DCSubatomicType, divisor uint32) *simpleParameter {
	sp := &simpleParameter{
		fieldBase:     newFieldBase(""),
		subatomicType: subatomicType,
		divisor:       1,
		nestedType:    STInvalid,
	}

	sp.packType = PTInvalid
	sp.hasNestedFields = false
	sp.numLengthBytes = 2

	switch subatomicType {
	case STInt8array:
		sp.packType = PTArray
		sp.nestedType = STInt8
		sp.hasNestedFields = true
		sp.bytesPerElement = 1
	case STInt16array:
		sp.packType = PTArray
		sp.nestedType = STInt16
		sp.hasNestedFields = true
		sp.bytesPerElement = 2
	case STInt32array:
		sp.packType = PTArray
		sp.nestedType = STInt32
		sp.hasNestedFields = true
		sp.bytesPerElement = 4
	case STUint8array:
		sp.packType = PTArray
		sp.nestedType = STUint8
		sp.hasNestedFields = true
		sp.bytesPerElement = 1
	case STUint16array:
		sp.packType = PTArray
		sp.nestedType = STUint16
		sp.hasNestedFields = true
		sp.bytesPerElement = 2
	case STUint32array:
		sp.packType = PTArray
		sp.nestedType = STUint32
		sp.hasNestedFields = true
		sp.bytesPerElement = 4
	case STUint32uint8array:
		sp.packType = PTArray
		sp.hasNestedFields = true
		sp.bytesPerElement = 5

	case STBlob32:
		sp.numLengthBytes = 4

		sp.packType = PTBlob
		sp.nestedType = STUint8
		sp.hasNestedFields = true
		sp.bytesPerElement = 1
	case STBlob:
		sp.numLengthBytes = 2
		sp.packType = PTBlob
		sp.nestedType = STUint8
		sp.hasNestedFields = true
		sp.bytesPerElement = 1

	case STString:
		sp.numLengthBytes = 2
		sp.packType = PTString
		sp.nestedType = STChar
		sp.hasNestedFields = true
		sp.bytesPerElement = 1

	case STInt8:
		sp.packType = PTInt
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 1
	case STInt16:
		sp.packType = PTInt
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 2
	case STInt32:
		sp.packType = PTInt
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 4
	case STInt64:
		sp.packType = PTInt64
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 8
	case STChar:
		sp.packType = PTString
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 1
	case STUint8:
		sp.packType = PTUint
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 1
	case STUint16:
		sp.packType = PTUint
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 2
	case STUint32:
		sp.packType = PTUint
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 4
	case STUint64:
		sp.packType = PTUint64
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 8
	case STFloat64:
		sp.packType = PTDouble
		sp.hasFixedByteSize = true
		sp.fixedByteSize = 8
	case STInvalid:

	}
	sp.hasFixedStructure = sp.hasFixedByteSize

	sp.setDivisor(divisor)

	if sp.nestedType != STInvalid {
		sp.nestedField = createNestedField(sp.nestedType, sp.divisor)
	} else if subatomicType == STUint32uint8array {

		sp.nestedField = createUint32Uint8Type()
	}

	return sp
}

func (sp *simpleParameter) GetType() DCSubatomicType { return sp.subatomicType }

func (sp *simpleParameter) HasModulus() bool { return sp.hasModulus }

func (sp *simpleParameter) GetModulus() float64 { return sp.origModulus }

func (sp *simpleParameter) GetDivisor() uint32 { return sp.divisor }

func (sp *simpleParameter) AsField() field     { return sp }
func (sp *simpleParameter) AsParameter() field { return sp }

func (sp *simpleParameter) GetDefaultValue() []byte         { return getDefaultValue(&sp.fieldBase, sp) }
func (sp *simpleParameter) ValidateRanges(data []byte) bool { return fieldValidateRanges(sp, data) }
func (sp *simpleParameter) FormatData(data []byte, showFieldNames ...bool) string {
	return fieldFormatData(sp, data, variadicBoolDefaultTrue(showFieldNames))
}
func (sp *simpleParameter) ParseString(s string) []byte { return fieldParseString(sp, s) }

func (sp *simpleParameter) MakeCopy() packerInterface {
	cp := *sp
	cp.keywordList = *newKeywordList()
	cp.keywordList.CopyKeywords(&sp.keywordList)
	return &cp
}

func (sp *simpleParameter) IsNumericType() bool {
	return !(sp.packType == PTString || sp.packType == PTBlob)
}

func (sp *simpleParameter) SetModulus(modulus float64) bool {
	if sp.packType == PTString || sp.packType == PTBlob || modulus <= 0.0 {
		return false
	}

	sp.hasModulus = true
	sp.origModulus = modulus

	rangeError := false
	sp.doubleModulus = modulus * float64(sp.divisor)
	sp.uint64Modulus = uint64(math.Floor(sp.doubleModulus + 0.5))
	sp.uintModulus = uint32(sp.uint64Modulus)

	switch sp.subatomicType {
	case STInt8, STInt8array:
		validateUint64Limits(sp.uint64Modulus-1, 7, &rangeError)
	case STInt16, STInt16array:
		validateUint64Limits(sp.uint64Modulus-1, 15, &rangeError)
	case STInt32, STInt32array:
		validateUint64Limits(sp.uint64Modulus-1, 31, &rangeError)
	case STInt64:
		validateUint64Limits(sp.uint64Modulus-1, 63, &rangeError)
	case STChar, STUint8, STUint8array:
		validateUint64Limits(sp.uint64Modulus-1, 8, &rangeError)
	case STUint16, STUint16array:
		validateUint64Limits(sp.uint64Modulus-1, 16, &rangeError)
	case STUint32, STUint32array:
		validateUint64Limits(sp.uint64Modulus-1, 32, &rangeError)
	case STUint64, STFloat64:

	default:
		return false
	}

	return !rangeError
}

func (sp *simpleParameter) SetDivisor(divisor uint32) bool {
	return sp.setDivisor(divisor)
}

func (sp *simpleParameter) setDivisor(divisor uint32) bool {
	if sp.packType == PTString || sp.packType == PTBlob || divisor == 0 {
		return false
	}

	sp.divisor = divisor
	if sp.divisor != 1 && (sp.packType == PTInt || sp.packType == PTInt64 ||
		sp.packType == PTUint || sp.packType == PTUint64) {
		sp.packType = PTDouble
	}

	if sp.hasRangeLimits {
		sp.SetRange(sp.origRange)
	}
	if sp.hasModulus {
		sp.SetModulus(sp.origModulus)
	}

	return true
}

func (sp *simpleParameter) SetRange(r dcDoubleRange) bool {
	rangeError := false
	numRanges := r.getNumRanges()
	sp.hasRangeLimits = numRanges != 0
	sp.origRange = r

	switch sp.subatomicType {
	case STInt8, STInt8array, STInt16, STInt16array, STInt32, STInt32array:
		numBits := map[DCSubatomicType]int{
			STInt8: 8, STInt8array: 8,
			STInt16: 16, STInt16array: 16,
			STInt32: 32, STInt32array: 32,
		}[sp.subatomicType]
		sp.intRange.clear()
		for i := 0; i < numRanges; i++ {
			min := int64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := int64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			validateInt64Limits(min, numBits, &rangeError)
			validateInt64Limits(max, numBits, &rangeError)
			sp.intRange.addRange(int(min), int(max))
		}

	case STInt64:
		sp.int64Range.clear()
		for i := 0; i < numRanges; i++ {
			min := int64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := int64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			sp.int64Range.addRange(min, max)
		}

	case STChar, STUint8, STUint8array, STUint16, STUint16array, STUint32, STUint32array:
		numBits := map[DCSubatomicType]int{
			STChar: 8, STUint8: 8, STUint8array: 8,
			STUint16: 16, STUint16array: 16,
			STUint32: 32, STUint32array: 32,
		}[sp.subatomicType]
		sp.uintRange.clear()
		for i := 0; i < numRanges; i++ {
			min := uint64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := uint64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			validateUint64Limits(min, numBits, &rangeError)
			validateUint64Limits(max, numBits, &rangeError)
			sp.uintRange.addRange(uint(min), uint(max))
		}

	case STUint64:
		sp.uint64Range.clear()
		for i := 0; i < numRanges; i++ {
			min := uint64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := uint64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			sp.uint64Range.addRange(min, max)
		}

	case STFloat64:
		sp.doubleRange.clear()
		for i := 0; i < numRanges; i++ {
			min := r.getMin(i) * float64(sp.divisor)
			max := r.getMax(i) * float64(sp.divisor)
			sp.doubleRange.addRange(min, max)
		}

	case STString, STBlob:
		sp.uintRange.clear()
		for i := 0; i < numRanges; i++ {
			min := uint64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := uint64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			validateUint64Limits(min, 16, &rangeError)
			validateUint64Limits(max, 16, &rangeError)
			sp.uintRange.addRange(uint(min), uint(max))
		}
		if sp.uintRange.hasOneValue() {

			sp.numLengthBytes = 0
			sp.hasFixedByteSize = true
			sp.fixedByteSize = int(sp.uintRange.getOneValue())
			sp.hasFixedStructure = true
		} else {
			sp.numLengthBytes = 2
			sp.hasFixedByteSize = false
			sp.hasFixedStructure = false
		}

	case STBlob32:
		sp.uintRange.clear()
		for i := 0; i < numRanges; i++ {
			min := uint64(math.Floor(r.getMin(i)*float64(sp.divisor) + 0.5))
			max := uint64(math.Floor(r.getMax(i)*float64(sp.divisor) + 0.5))
			validateUint64Limits(min, 32, &rangeError)
			validateUint64Limits(max, 32, &rangeError)
			sp.uintRange.addRange(uint(min), uint(max))
		}
		if sp.uintRange.hasOneValue() {

			sp.numLengthBytes = 0
			sp.hasFixedByteSize = true
			sp.fixedByteSize = int(sp.uintRange.getOneValue())
			sp.hasFixedStructure = true
		} else {
			sp.numLengthBytes = 4
			sp.hasFixedByteSize = false
			sp.hasFixedStructure = false
		}

	default:
		return false
	}

	return !rangeError
}

func (sp *simpleParameter) CalcNumNestedFields(lengthBytes int) int {
	if sp.bytesPerElement != 0 {
		return lengthBytes / sp.bytesPerElement
	}
	return 0
}

func (sp *simpleParameter) GetNestedField(int) packerInterface {
	return sp.nestedField
}

func (sp *simpleParameter) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (sp *simpleParameter) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	realValue := value * float64(sp.divisor)
	if sp.hasModulus {
		if realValue < 0.0 {
			realValue = sp.doubleModulus - math.Mod(-realValue, sp.doubleModulus)
			if realValue == sp.doubleModulus {
				realValue = 0.0
			}
		} else {
			realValue = math.Mod(realValue, sp.doubleModulus)
		}
	}

	switch sp.subatomicType {
	case STInt8:
		intValue := int(math.Floor(realValue + 0.5))
		sp.intRange.validate(intValue, rangeError)
		validateIntLimits(intValue, 8, rangeError)
		doPackInt8(pd.getWritePointer(1), intValue)
	case STInt16:
		intValue := int(math.Floor(realValue + 0.5))
		sp.intRange.validate(intValue, rangeError)
		validateIntLimits(intValue, 16, rangeError)
		doPackInt16(pd.getWritePointer(2), intValue)
	case STInt32:
		intValue := int(math.Floor(realValue + 0.5))
		sp.intRange.validate(intValue, rangeError)
		doPackInt32(pd.getWritePointer(4), intValue)
	case STInt64:
		int64Value := int64(math.Floor(realValue + 0.5))
		sp.int64Range.validate(int64Value, rangeError)
		doPackInt64(pd.getWritePointer(8), int64Value)
	case STChar, STUint8:
		intValue := uint(math.Floor(realValue + 0.5))
		sp.uintRange.validate(intValue, rangeError)
		validateUintLimits(intValue, 8, rangeError)
		doPackUint8(pd.getWritePointer(1), intValue)
	case STUint16:
		intValue := uint(math.Floor(realValue + 0.5))
		sp.uintRange.validate(intValue, rangeError)
		validateUintLimits(intValue, 16, rangeError)
		doPackUint16(pd.getWritePointer(2), intValue)
	case STUint32:
		intValue := uint(math.Floor(realValue + 0.5))
		sp.uintRange.validate(intValue, rangeError)
		doPackUint32(pd.getWritePointer(4), intValue)
	case STUint64:
		int64Value := uint64(math.Floor(realValue + 0.5))
		sp.uint64Range.validate(int64Value, rangeError)
		doPackUint64(pd.getWritePointer(8), int64Value)
	case STFloat64:
		sp.doubleRange.validate(realValue, rangeError)
		doPackFloat64(pd.getWritePointer(8), realValue)
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackInt(pd *packData, value int, packError, rangeError *bool) {
	intValue := value * int(sp.divisor)

	if value != 0 && intValue/value != int(sp.divisor) {

		sp.PackInt64(pd, int64(value), packError, rangeError)
		return
	}

	if sp.hasModulus && sp.uintModulus != 0 {
		if intValue < 0 {
			intValue = int(sp.uintModulus) - 1 - (-intValue-1)%int(sp.uintModulus)
		} else {
			intValue = intValue % int(sp.uintModulus)
		}
	}

	switch sp.subatomicType {
	case STInt8:
		sp.intRange.validate(intValue, rangeError)
		validateIntLimits(intValue, 8, rangeError)
		doPackInt8(pd.getWritePointer(1), intValue)
	case STInt16:
		sp.intRange.validate(intValue, rangeError)
		validateIntLimits(intValue, 16, rangeError)
		doPackInt16(pd.getWritePointer(2), intValue)
	case STInt32:
		sp.intRange.validate(intValue, rangeError)
		doPackInt32(pd.getWritePointer(4), intValue)
	case STInt64:
		sp.int64Range.validate(int64(intValue), rangeError)
		doPackInt64(pd.getWritePointer(8), int64(intValue))
	case STChar, STUint8:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(intValue), rangeError)
		validateUintLimits(uint(intValue), 8, rangeError)
		doPackUint8(pd.getWritePointer(1), uint(intValue))
	case STUint16:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(intValue), rangeError)
		validateUintLimits(uint(intValue), 16, rangeError)
		doPackUint16(pd.getWritePointer(2), uint(intValue))
	case STUint32:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(intValue), rangeError)
		doPackUint32(pd.getWritePointer(4), uint(intValue))
	case STUint64:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uint64Range.validate(uint64(intValue), rangeError)
		doPackUint64(pd.getWritePointer(8), uint64(intValue))
	case STFloat64:
		sp.doubleRange.validate(float64(intValue), rangeError)
		doPackFloat64(pd.getWritePointer(8), float64(intValue))
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	intValue := value * uint(sp.divisor)
	if sp.hasModulus && sp.uintModulus != 0 {
		intValue = intValue % uint(sp.uintModulus)
	}

	switch sp.subatomicType {
	case STInt8:
		if int(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(intValue), rangeError)
		validateIntLimits(int(intValue), 8, rangeError)
		doPackInt8(pd.getWritePointer(1), int(intValue))
	case STInt16:
		if int(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(intValue), rangeError)
		validateIntLimits(int(intValue), 16, rangeError)
		doPackInt16(pd.getWritePointer(2), int(intValue))
	case STInt32:
		if int(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(intValue), rangeError)
		doPackInt32(pd.getWritePointer(4), int(intValue))
	case STInt64:
		if int(intValue) < 0 {
			*rangeError = true
		}
		sp.int64Range.validate(int64(int(intValue)), rangeError)
		doPackInt64(pd.getWritePointer(8), int64(int(intValue)))
	case STChar, STUint8:
		sp.uintRange.validate(intValue, rangeError)
		validateUintLimits(intValue, 8, rangeError)
		doPackUint8(pd.getWritePointer(1), intValue)
	case STUint16:
		sp.uintRange.validate(intValue, rangeError)
		validateUintLimits(intValue, 16, rangeError)
		doPackUint16(pd.getWritePointer(2), intValue)
	case STUint32:
		sp.uintRange.validate(intValue, rangeError)
		doPackUint32(pd.getWritePointer(4), intValue)
	case STUint64:
		sp.uint64Range.validate(uint64(intValue), rangeError)
		doPackUint64(pd.getWritePointer(8), uint64(intValue))
	case STFloat64:
		sp.doubleRange.validate(float64(intValue), rangeError)
		doPackFloat64(pd.getWritePointer(8), float64(intValue))
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	intValue := value * int64(sp.divisor)
	if sp.hasModulus && sp.uint64Modulus != 0 {
		if intValue < 0 {
			intValue = int64(sp.uint64Modulus) - 1 - (-intValue-1)%int64(sp.uint64Modulus)
		} else {
			intValue = intValue % int64(sp.uint64Modulus)
		}
	}

	switch sp.subatomicType {
	case STInt8:
		sp.intRange.validate(int(intValue), rangeError)
		validateInt64Limits(intValue, 8, rangeError)
		doPackInt8(pd.getWritePointer(1), int(intValue))
	case STInt16:
		sp.intRange.validate(int(intValue), rangeError)
		validateInt64Limits(intValue, 16, rangeError)
		doPackInt16(pd.getWritePointer(2), int(intValue))
	case STInt32:
		sp.intRange.validate(int(intValue), rangeError)
		validateInt64Limits(intValue, 32, rangeError)
		doPackInt32(pd.getWritePointer(4), int(intValue))
	case STInt64:
		sp.int64Range.validate(intValue, rangeError)
		doPackInt64(pd.getWritePointer(8), intValue)
	case STChar, STUint8:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(uint64(intValue)), rangeError)
		validateUint64Limits(uint64(intValue), 8, rangeError)
		doPackUint8(pd.getWritePointer(1), uint(uint64(intValue)))
	case STUint16:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(uint64(intValue)), rangeError)
		validateUint64Limits(uint64(intValue), 16, rangeError)
		doPackUint16(pd.getWritePointer(2), uint(uint64(intValue)))
	case STUint32:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uintRange.validate(uint(uint64(intValue)), rangeError)
		validateUint64Limits(uint64(intValue), 32, rangeError)
		doPackUint32(pd.getWritePointer(4), uint(uint64(intValue)))
	case STUint64:
		if intValue < 0 {
			*rangeError = true
		}
		sp.uint64Range.validate(uint64(intValue), rangeError)
		doPackUint64(pd.getWritePointer(8), uint64(intValue))
	case STFloat64:
		sp.doubleRange.validate(float64(intValue), rangeError)
		doPackFloat64(pd.getWritePointer(8), float64(intValue))
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	intValue := value * uint64(sp.divisor)
	if sp.hasModulus && sp.uint64Modulus != 0 {
		intValue = intValue % sp.uint64Modulus
	}

	switch sp.subatomicType {
	case STInt8:
		if int64(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(int64(intValue)), rangeError)
		validateInt64Limits(int64(intValue), 8, rangeError)
		doPackInt8(pd.getWritePointer(1), int(int64(intValue)))
	case STInt16:
		if int64(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(int64(intValue)), rangeError)
		validateInt64Limits(int64(intValue), 16, rangeError)
		doPackInt16(pd.getWritePointer(2), int(int64(intValue)))
	case STInt32:
		if int64(intValue) < 0 {
			*rangeError = true
		}
		sp.intRange.validate(int(int64(intValue)), rangeError)
		validateInt64Limits(int64(intValue), 32, rangeError)
		doPackInt32(pd.getWritePointer(4), int(int64(intValue)))
	case STInt64:
		if int64(intValue) < 0 {
			*rangeError = true
		}
		sp.int64Range.validate(int64(intValue), rangeError)
		doPackInt64(pd.getWritePointer(8), int64(intValue))
	case STChar, STUint8:
		sp.uintRange.validate(uint(intValue), rangeError)
		validateUint64Limits(intValue, 8, rangeError)
		doPackUint8(pd.getWritePointer(1), uint(intValue))
	case STUint16:
		sp.uintRange.validate(uint(intValue), rangeError)
		validateUint64Limits(intValue, 16, rangeError)
		doPackUint16(pd.getWritePointer(2), uint(intValue))
	case STUint32:
		sp.uintRange.validate(uint(intValue), rangeError)
		validateUint64Limits(intValue, 32, rangeError)
		doPackUint32(pd.getWritePointer(4), uint(intValue))
	case STUint64:
		sp.uint64Range.validate(intValue, rangeError)
		doPackUint64(pd.getWritePointer(8), intValue)
	case STFloat64:
		sp.doubleRange.validate(float64(intValue), rangeError)
		doPackFloat64(pd.getWritePointer(8), float64(intValue))
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackString(pd *packData, value string, packError, rangeError *bool) {
	stringLength := len(value)

	switch sp.subatomicType {
	case STChar, STUint8, STInt8:
		if stringLength == 0 {
			*packError = true
		} else {
			if stringLength != 1 {
				*rangeError = true
			}
			sp.uintRange.validate(uint(value[0]), rangeError)
			doPackUint8(pd.getWritePointer(1), uint(value[0]))
		}
	case STString, STBlob:
		sp.uintRange.validate(uint(stringLength), rangeError)
		validateUintLimits(uint(stringLength), 16, rangeError)
		if sp.numLengthBytes != 0 {
			doPackUint16(pd.getWritePointer(2), uint(stringLength))
		}
		pd.appendData([]byte(value))
	case STBlob32:
		sp.uintRange.validate(uint(stringLength), rangeError)
		if sp.numLengthBytes != 0 {
			doPackUint32(pd.getWritePointer(4), uint(stringLength))
		}
		pd.appendData([]byte(value))
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	blobSize := len(value)

	switch sp.subatomicType {
	case STChar, STUint8, STInt8:
		if blobSize == 0 {
			*packError = true
		} else {
			if blobSize != 1 {
				*rangeError = true
			}
			sp.uintRange.validate(uint(value[0]), rangeError)
			doPackUint8(pd.getWritePointer(1), uint(value[0]))
		}
	case STString, STBlob:
		sp.uintRange.validate(uint(blobSize), rangeError)
		validateUintLimits(uint(blobSize), 16, rangeError)
		if sp.numLengthBytes != 0 {
			doPackUint16(pd.getWritePointer(2), uint(blobSize))
		}
		pd.appendData(value)
	case STBlob32:
		sp.uintRange.validate(uint(blobSize), rangeError)
		if sp.numLengthBytes != 0 {
			doPackUint32(pd.getWritePointer(4), uint(blobSize))
		}
		pd.appendData(value)
	default:
		*packError = true
	}
}

func (sp *simpleParameter) PackDefaultValue(pd *packData, packError *bool) bool {
	if sp.HasDefaultValue() {
		return fieldPackDefaultValue(&sp.fieldBase, pd, packError)
	}

	if sp.hasNestedFields {

		minimumLength := 0
		if !sp.uintRange.isEmpty() {
			minimumLength = int(sp.uintRange.getMin(0))
		}

		p := newDCPacker()
		p.beginPack(sp)
		p.push()
		for i := 0; i < minimumLength; i++ {
			p.packDefaultValue()
		}
		p.pop()
		if !p.endPack() {
			*packError = true
		} else {
			pd.appendData(p.getData())
		}
	} else {

		switch sp.subatomicType {
		case STInt8, STInt16, STInt32:
			if sp.intRange.isInRange(0) {
				sp.PackInt(pd, 0, packError, packError)
			} else {
				sp.PackInt(pd, sp.intRange.getMin(0), packError, packError)
			}
		case STInt64:
			if sp.int64Range.isInRange(0) {
				sp.PackInt64(pd, 0, packError, packError)
			} else {
				sp.PackInt64(pd, sp.int64Range.getMin(0), packError, packError)
			}
		case STChar, STUint8, STUint16, STUint32:
			if sp.uintRange.isInRange(0) {
				sp.PackUint(pd, 0, packError, packError)
			} else {
				sp.PackUint(pd, sp.uintRange.getMin(0), packError, packError)
			}
		case STUint64:
			if sp.uint64Range.isInRange(0) {
				sp.PackUint64(pd, 0, packError, packError)
			} else {
				sp.PackUint64(pd, sp.uint64Range.getMin(0), packError, packError)
			}
		case STFloat64:
			if sp.doubleRange.isInRange(0) {
				sp.PackDouble(pd, 0, packError, packError)
			} else {
				sp.PackDouble(pd, sp.doubleRange.getMin(0), packError, packError)
			}
		default:
			*packError = true
		}
	}
	return true
}

func (sp *simpleParameter) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt8(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = float64(intValue)
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt16(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = float64(intValue)
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt32(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = float64(intValue)
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt64(data[*p:])
		sp.int64Range.validate(intValue, rangeError)
		*value = float64(intValue)
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = float64(uintValue)
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint16(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = float64(uintValue)
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint32(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = float64(uintValue)
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint64(data[*p:])
		sp.uint64Range.validate(uintValue, rangeError)
		*value = float64(uintValue)
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackFloat64(data[*p:])
		sp.doubleRange.validate(*value, rangeError)
		*p += 8
	default:
		*packError = true
		return
	}

	if sp.divisor != 1 {
		*value = *value / float64(sp.divisor)
	}
}

func (sp *simpleParameter) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackInt8(data[*p:])
		sp.intRange.validate(*value, rangeError)
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackInt16(data[*p:])
		sp.intRange.validate(*value, rangeError)
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackInt32(data[*p:])
		sp.intRange.validate(*value, rangeError)
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		int64Value := doUnpackInt64(data[*p:])
		sp.int64Range.validate(int64Value, rangeError)
		*value = int(int64Value)
		if int64(*value) != int64Value {

			*packError = true
		}
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int(uintValue)
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint16(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int(uintValue)
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint32(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int(uintValue)
		if *value < 0 {
			*packError = true
		}
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint64(data[*p:])
		sp.uint64Range.validate(uintValue, rangeError)
		*value = int(uint(uintValue))
		if uint64(uint(*value)) != uintValue || *value < 0 {
			*packError = true
		}
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		realValue := doUnpackFloat64(data[*p:])
		sp.doubleRange.validate(realValue, rangeError)
		*value = int(realValue)
		*p += 8
	default:
		*packError = true
		return
	}

	if sp.divisor != 1 {
		*value = *value / int(sp.divisor)
	}
}

func (sp *simpleParameter) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt8(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint(intValue)
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt16(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint(intValue)
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt32(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint(intValue)
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt64(data[*p:])
		sp.int64Range.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint(int(intValue))
		if uint64(*value) != uint64(intValue) {
			*packError = true
		}
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackUint8(data[*p:])
		sp.uintRange.validate(*value, rangeError)
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackUint16(data[*p:])
		sp.uintRange.validate(*value, rangeError)
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackUint32(data[*p:])
		sp.uintRange.validate(*value, rangeError)
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint64(data[*p:])
		sp.uint64Range.validate(uintValue, rangeError)
		*value = uint(uintValue)
		if uint64(*value) != uintValue {
			*packError = true
		}
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		realValue := doUnpackFloat64(data[*p:])
		sp.doubleRange.validate(realValue, rangeError)
		*value = uint(realValue)
		*p += 8
	default:
		*packError = true
		return
	}

	if sp.divisor != 1 {
		*value = *value / uint(sp.divisor)
	}
}

func (sp *simpleParameter) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt8(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = int64(intValue)
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt16(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = int64(intValue)
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt32(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		*value = int64(intValue)
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackInt64(data[*p:])
		sp.int64Range.validate(*value, rangeError)
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int64(int(uintValue))
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint16(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int64(int(uintValue))
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint32(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = int64(int(uintValue))
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint64(data[*p:])
		sp.uint64Range.validate(uintValue, rangeError)
		*value = int64(uintValue)
		if *value < 0 {
			*packError = true
		}
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		realValue := doUnpackFloat64(data[*p:])
		sp.doubleRange.validate(realValue, rangeError)
		*value = int64(realValue)
		*p += 8
	default:
		*packError = true
		return
	}

	if sp.divisor != 1 {
		*value = *value / int64(sp.divisor)
	}
}

func (sp *simpleParameter) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt8(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint64(uint(intValue))
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt16(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint64(uint(intValue))
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt32(data[*p:])
		sp.intRange.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint64(uint(intValue))
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		intValue := doUnpackInt64(data[*p:])
		sp.int64Range.validate(intValue, rangeError)
		if intValue < 0 {
			*packError = true
		}
		*value = uint64(int(intValue))
		if *value != uint64(intValue) {
			*packError = true
		}
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = uint64(uintValue)
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint16(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = uint64(uintValue)
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint32(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = uint64(uintValue)
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		*value = doUnpackUint64(data[*p:])
		sp.uint64Range.validate(*value, rangeError)
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return
		}
		realValue := doUnpackFloat64(data[*p:])
		sp.doubleRange.validate(realValue, rangeError)
		*value = uint64(realValue)
		*p += 8
	default:
		*packError = true
		return
	}

	if sp.divisor != 1 {
		*value = *value / uint64(sp.divisor)
	}
}

func (sp *simpleParameter) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {

	switch sp.subatomicType {
	case STChar, STInt8, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = string([]byte{byte(uintValue)})
		*p++
		return
	}

	var stringLength int
	if sp.numLengthBytes == 0 {
		stringLength = sp.fixedByteSize
	} else {
		switch sp.subatomicType {
		case STString, STBlob:
			if *p+2 > len(data) {
				*packError = true
				return
			}
			stringLength = int(doUnpackUint16(data[*p:]))
			*p += 2
		case STBlob32:
			if *p+4 > len(data) {
				*packError = true
				return
			}
			stringLength = int(doUnpackUint32(data[*p:]))
			*p += 4
		default:
			*packError = true
			return
		}
	}

	sp.uintRange.validate(uint(stringLength), rangeError)

	if *p+stringLength > len(data) {
		*packError = true
		return
	}
	*value = string(data[*p : *p+stringLength])
	*p += stringLength
}

func (sp *simpleParameter) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {

	switch sp.subatomicType {
	case STChar, STInt8, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return
		}
		uintValue := doUnpackUint8(data[*p:])
		sp.uintRange.validate(uintValue, rangeError)
		*value = []byte{byte(uintValue)}
		*p++
		return
	}

	var blobSize int
	if sp.numLengthBytes == 0 {
		blobSize = sp.fixedByteSize
	} else {
		switch sp.subatomicType {
		case STString, STBlob:
			if *p+2 > len(data) {
				*packError = true
				return
			}
			blobSize = int(doUnpackUint16(data[*p:]))
			*p += 2
		case STBlob32:
			if *p+4 > len(data) {
				*packError = true
				return
			}
			blobSize = int(doUnpackUint32(data[*p:]))
			*p += 4
		default:
			*packError = true
			return
		}
	}

	sp.uintRange.validate(uint(blobSize), rangeError)

	if *p+blobSize > len(data) {
		*packError = true
		return
	}
	*value = append([]byte(nil), data[*p:*p+blobSize]...)
	*p += blobSize
}

func (sp *simpleParameter) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	if !sp.hasRangeLimits {
		return sp.UnpackSkip(data, p, packError)
	}

	switch sp.subatomicType {
	case STInt8:
		if *p+1 > len(data) {
			*packError = true
			return true
		}
		sp.intRange.validate(doUnpackInt8(data[*p:]), rangeError)
		*p++
	case STInt16:
		if *p+2 > len(data) {
			*packError = true
			return true
		}
		sp.intRange.validate(doUnpackInt16(data[*p:]), rangeError)
		*p += 2
	case STInt32:
		if *p+4 > len(data) {
			*packError = true
			return true
		}
		sp.intRange.validate(doUnpackInt32(data[*p:]), rangeError)
		*p += 4
	case STInt64:
		if *p+8 > len(data) {
			*packError = true
			return true
		}
		sp.int64Range.validate(doUnpackInt64(data[*p:]), rangeError)
		*p += 8
	case STChar, STUint8:
		if *p+1 > len(data) {
			*packError = true
			return true
		}
		sp.uintRange.validate(doUnpackUint8(data[*p:]), rangeError)
		*p++
	case STUint16:
		if *p+2 > len(data) {
			*packError = true
			return true
		}
		sp.uintRange.validate(doUnpackUint16(data[*p:]), rangeError)
		*p += 2
	case STUint32:
		if *p+4 > len(data) {
			*packError = true
			return true
		}
		sp.uintRange.validate(doUnpackUint32(data[*p:]), rangeError)
		*p += 4
	case STUint64:
		if *p+8 > len(data) {
			*packError = true
			return true
		}
		sp.uint64Range.validate(doUnpackUint64(data[*p:]), rangeError)
		*p += 8
	case STFloat64:
		if *p+8 > len(data) {
			*packError = true
			return true
		}
		sp.doubleRange.validate(doUnpackFloat64(data[*p:]), rangeError)
		*p += 8
	case STString, STBlob:
		if sp.numLengthBytes == 0 {
			*p += sp.fixedByteSize
		} else {
			if *p+2 > len(data) {
				*packError = true
				return true
			}
			stringLength := int(doUnpackUint16(data[*p:]))
			sp.uintRange.validate(uint(stringLength), rangeError)
			*p += 2 + stringLength
		}
	case STBlob32:
		if sp.numLengthBytes == 0 {
			*p += sp.fixedByteSize
		} else {
			if *p+4 > len(data) {
				*packError = true
				return true
			}
			stringLength := int(doUnpackUint32(data[*p:]))
			sp.uintRange.validate(uint(stringLength), rangeError)
			*p += 4 + stringLength
		}
	default:
		return false
	}

	return true
}

func (sp *simpleParameter) UnpackSkip(data []byte, p *int, packError *bool) bool {
	switch sp.subatomicType {
	case STChar, STInt8, STUint8:
		*p++
	case STInt16, STUint16:
		*p += 2
	case STInt32, STUint32:
		*p += 4
	case STInt64, STUint64, STFloat64:
		*p += 8
	case STString, STBlob:
		if sp.numLengthBytes == 0 {
			*p += sp.fixedByteSize
		} else {
			if *p+2 > len(data) {
				return false
			}
			stringLength := int(doUnpackUint16(data[*p:]))
			*p += 2 + stringLength
		}
	case STBlob32:
		if sp.numLengthBytes == 0 {
			*p += sp.fixedByteSize
		} else {
			if *p+4 > len(data) {
				return false
			}
			stringLength := int(doUnpackUint32(data[*p:]))
			*p += 4 + stringLength
		}
	default:
		return false
	}

	if *p > len(data) {
		*packError = true
	}
	return true
}

func (sp *simpleParameter) GenerateHash(hashgen *hashGenerator) {

	if sp.GetNumKeywords() != 0 {
		sp.keywordList.generateHash(hashgen)
	}

	hashgen.addInt(int32(sp.subatomicType))
	hashgen.addInt(int32(sp.divisor))
	if sp.hasModulus {
		hashgen.addInt(int32(sp.doubleModulus))
	}

	sp.intRange.generateHash(hashgen)
	sp.int64Range.generateHash(hashgen)
	sp.uintRange.generateHash(hashgen)
	sp.uint64Range.generateHash(hashgen)
	sp.doubleRange.generateHash(hashgen)
}

func (sp *simpleParameter) DoCheckMatch(other packerInterface) bool {
	switch o := other.(type) {
	case *simpleParameter:
		return sp.doCheckMatchSimpleParameter(o)
	case *arrayParameter:
		return sp.doCheckMatchArrayParameter(o)
	default:
		return false
	}
}

func (sp *simpleParameter) doCheckMatchSimpleParameter(other *simpleParameter) bool {
	if sp.divisor != other.divisor {
		return false
	}
	if sp.subatomicType == other.subatomicType {
		return true
	}

	switch sp.subatomicType {
	case STUint8, STChar:
		switch other.subatomicType {
		case STUint8, STChar:
			return true
		default:
			return false
		}
	case STString, STBlob, STUint8array:
		switch other.subatomicType {
		case STString, STBlob, STUint8array:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func (sp *simpleParameter) doCheckMatchArrayParameter(other *arrayParameter) bool {
	if other.arraySize != -1 {

		return false
	}
	if sp.nestedField == nil {

		return false
	}
	return sp.nestedField.DoCheckMatch(other.elementType)
}

func createNestedField(subatomicType DCSubatomicType, divisor uint32) *simpleParameter {
	divisorMap, ok := nestedFieldMap[subatomicType]
	if !ok {
		divisorMap = make(map[uint32]*simpleParameter)
		nestedFieldMap[subatomicType] = divisorMap
	}
	if existing, ok := divisorMap[divisor]; ok {
		return existing
	}
	nested := newSimpleParameter(subatomicType, divisor)
	divisorMap[divisor] = nested
	return nested
}
