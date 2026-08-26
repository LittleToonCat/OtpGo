package dc

import "strings"

func (p *dcPacker) ClearData() { p.clearData() }

func (p *dcPacker) SetUnpackData(data Vector) { p.setUnpackData(data) }

func (p *dcPacker) BeginPack(root DCField) { p.beginPack(root) }

func (p *dcPacker) EndPack() bool { return p.endPack() }

func (p *dcPacker) BeginUnpack(root DCField) { p.beginUnpack(root) }

func (p *dcPacker) EndUnpack() bool { return p.endUnpack() }

func (p *dcPacker) Push() { p.push() }

func (p *dcPacker) Pop() { p.pop() }

func (p *dcPacker) HasNestedFields() bool { return p.hasNestedFields() }

func (p *dcPacker) GetNumNestedFields() int { return p.getNumNestedFields() }

func (p *dcPacker) MoreNestedFields() bool { return p.moreNestedFields() }

func (p *dcPacker) GetPackType() DCPackType { return p.getPackType() }

func (p *dcPacker) GetCurrentFieldName() string { return p.getCurrentFieldName() }

func (p *dcPacker) GetCurrentField() DCField {
	f, _ := p.getCurrentField().(field)
	return f
}

func (p *dcPacker) PackDouble(value float64) { p.packDouble(value) }

func (p *dcPacker) PackInt(value int) { p.packInt(value) }

func (p *dcPacker) PackInt64(value int64) { p.packInt64(value) }

func (p *dcPacker) PackUint(value uint) { p.packUint(value) }

func (p *dcPacker) PackUint64(value uint64) { p.packUint64(value) }

func (p *dcPacker) PackString(value string) { p.packString(value) }

func (p *dcPacker) PackBlob(value []byte) { p.packBlob(value) }

func (p *dcPacker) PackDefaultValue() { p.packDefaultValue() }

func (p *dcPacker) PackLiteralValue(value []byte) { p.packLiteralValue(value) }

func (p *dcPacker) UnpackDouble() float64 { return p.unpackDouble() }

func (p *dcPacker) UnpackInt() int { return p.unpackInt() }

func (p *dcPacker) UnpackInt64() int64 { return p.unpackInt64() }

func (p *dcPacker) UnpackUint() uint { return p.unpackUint() }

func (p *dcPacker) UnpackUint64() uint64 { return p.unpackUint64() }

func (p *dcPacker) UnpackString() string { return p.unpackString() }

func (p *dcPacker) UnpackBlob() Vector { return p.unpackBlob() }

func (p *dcPacker) UnpackLiteralValue() Vector { return p.unpackLiteralValue() }

func (p *dcPacker) UnpackValidate() { p.unpackValidate() }

func (p *dcPacker) UnpackSkip() { p.unpackSkip() }

func (p *dcPacker) RawPackUint16(value uint) { p.rawPackUint16(value) }

func (p *dcPacker) RawUnpackUint16() uint { return p.rawUnpackUint16() }

func (p *dcPacker) HadParseError() bool { return p.parseError }

func (p *dcPacker) HadPackError() bool { return p.hadPackError() }

func (p *dcPacker) HadRangeError() bool { return p.hadRangeError() }

func (p *dcPacker) HadError() bool { return p.hadError() }

func (p *dcPacker) GetLength() int { return p.getLength() }

func (p *dcPacker) GetString() string { return p.getString() }

func (p *dcPacker) GetBytes() Vector { return append(Vector(nil), p.getData()...) }
func (p *dcPacker) GetData() []byte  { return append([]byte(nil), p.getData()...) }

func (p *dcPacker) GetUnpackLength() int { return p.getUnpackLength() }

func (p *dcPacker) GetUnpackString() string { return p.getUnpackString() }

func (p *dcPacker) GetNumUnpackedBytes() int { return p.getNumUnpackedBytes() }

func (p *dcPacker) ParseAndPack(formattedObject string) bool {
	if p.currentField == nil {
		p.packError = true
		return false
	}
	pr := newParser([]byte(formattedObject), nil)
	pr.parseParameterValue(p)
	if len(pr.errors) != 0 || len(pr.lx.errors) != 0 {
		p.parseError = true
	}
	return !p.parseError
}

func (p *dcPacker) UnpackAndFormat(showFieldNames ...bool) string {
	var out strings.Builder
	p.unpackAndFormat(&out, variadicBoolDefaultTrue(showFieldNames))
	return out.String()
}
