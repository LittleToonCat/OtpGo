package dc

import "strings"

type field interface {
	packerInterface
	GetName() string
	GetNumber() int
	SetNumber(int)
	GetClass() *dcClass
	SetClass(*dcClass)
	IsBogusField() bool
	HasDefaultValue() bool
	GetDefaultValue() Vector
	HasKeyword(name string) bool
	HasKeywordName(string) bool
	GetNumKeywords() int
	IsRequired() bool
	IsBroadcast() bool
	IsRam() bool
	IsDb() bool
	IsClsend() bool
	IsClrecv() bool
	IsOwnsend() bool
	IsOwnrecv() bool
	IsAirecv() bool

	AsField() field
	AsAtomicField() *atomicField
	AsMolecularField() *molecularField
	AsParameter() field

	ValidateRanges(data Vector) bool
	FormatData(data Vector, showFieldNames ...bool) string
	ParseString(s string) Vector
}

type fieldBase struct {
	packerBase
	keywordList

	class_            *dcClass
	number            int
	bogusField        bool
	fromTypedef       bool
	hasDefaultValue   bool
	defaultValueStale bool
	defaultValue      []byte
}

func newFieldBaseRaw(name string) fieldBase {
	fb := fieldBase{
		packerBase:        newPackerBase(name),
		keywordList:       *newKeywordList(),
		number:            -1,
		defaultValueStale: true,
	}
	fb.packerBase.hasNestedFields = true
	fb.packerBase.numNestedFields = 0
	fb.packerBase.packType = PTField
	fb.packerBase.hasFixedByteSize = true
	fb.packerBase.fixedByteSize = 0
	fb.packerBase.hasFixedStructure = true
	return fb
}

func newFieldBase(name string) fieldBase {
	fb := newFieldBaseRaw(name)
	fb.packerBase.hasFixedByteSize = false
	fb.packerBase.hasFixedStructure = false
	fb.packerBase.numNestedFields = -1
	return fb
}

func (fb *fieldBase) GetNumber() int { return fb.number }

func (fb *fieldBase) SetNumber(number int) { fb.number = number }

func (fb *fieldBase) GetClass() *dcClass { return fb.class_ }

func (fb *fieldBase) SetClass(c *dcClass) { fb.class_ = c }

func (fb *fieldBase) IsBogusField() bool { return fb.bogusField }

func (fb *fieldBase) IsFromTypedef() bool { return fb.fromTypedef }

func (fb *fieldBase) SetFromTypedef(v bool) { fb.fromTypedef = v }

func (fb *fieldBase) GetName() string { return fb.Name() }

func (fb *fieldBase) HasKeyword(name string) bool { return fb.HasKeywordName(name) }

func (fb *fieldBase) HasDefaultValue() bool { return fb.hasDefaultValue }

func (fb *fieldBase) SetDefaultValue(value []byte) {
	fb.defaultValue = value
	fb.hasDefaultValue = true
	fb.defaultValueStale = false
}

func getDefaultValue(fb *fieldBase, self packerInterface) []byte {
	if fb.defaultValueStale {
		refreshDefaultValue(fb, self)
	}
	return fb.defaultValue
}

func refreshDefaultValue(fb *fieldBase, self packerInterface) {
	p := newDCPacker()
	p.beginPack(self)
	p.packDefaultValue()
	if p.endPack() {
		fb.defaultValue = append([]byte(nil), p.getData()...)
	}
	fb.defaultValueStale = false
}

func (fb *fieldBase) IsRequired() bool { return fb.HasKeywordName("required") }

func (fb *fieldBase) IsBroadcast() bool { return fb.HasKeywordName("broadcast") }

func (fb *fieldBase) IsRam() bool { return fb.HasKeywordName("ram") }

func (fb *fieldBase) IsDb() bool { return fb.HasKeywordName("db") }

func (fb *fieldBase) IsClsend() bool { return fb.HasKeywordName("clsend") }

func (fb *fieldBase) IsClrecv() bool { return fb.HasKeywordName("clrecv") }

func (fb *fieldBase) IsOwnsend() bool { return fb.HasKeywordName("ownsend") }

func (fb *fieldBase) IsOwnrecv() bool { return fb.HasKeywordName("ownrecv") }

func (fb *fieldBase) IsAirecv() bool { return fb.HasKeywordName("airecv") }

func (fb *fieldBase) AsAtomicField() *atomicField { return nil }

func (fb *fieldBase) AsMolecularField() *molecularField { return nil }

func (fb *fieldBase) AsParameter() field { return nil }

func fieldValidateRanges(self packerInterface, data []byte) bool {
	p := newDCPacker()
	p.setUnpackData(data)
	p.beginUnpack(self)
	p.unpackValidate()
	if !p.endUnpack() {
		return false
	}
	return p.getNumUnpackedBytes() == len(data)
}

func fieldFormatData(self packerInterface, data []byte, showFieldNames bool) string {
	p := newDCPacker()
	p.setUnpackData(data)
	p.beginUnpack(self)
	var out strings.Builder
	p.unpackAndFormat(&out, showFieldNames)
	if !p.endUnpack() {
		return ""
	}
	return out.String()
}

func fieldParseString(self packerInterface, s string) []byte {
	data, ok := parseAndPackValue([]byte(s), self)
	if !ok {
		return nil
	}
	return data
}

func variadicBoolDefaultTrue(args []bool) bool {
	if len(args) == 0 {
		return true
	}
	return args[0]
}

func fieldPackDefaultValue(fb *fieldBase, pd *packData, packError *bool) bool {

	if !fb.defaultValueStale {
		pd.appendData(fb.defaultValue)
		return true
	}
	return false
}

func fieldGenerateHash(fb *fieldBase, hashgen *hashGenerator) {

	hashgen.addString(fb.name)

	if fb.class_ != nil && fb.class_.dcFile != nil && fb.class_.dcFile.GetMultipleInheritance() {
		hashgen.addInt(int32(fb.number))
	}
}
