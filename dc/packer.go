package dc

type packerMode int

const (
	modeIdle packerMode = iota
	modePack
	modeUnpack
)

type packerStackElement struct {
	currentParent     packerInterface
	currentFieldIndex int
	pushMarker        int
	popMarker         int
	next              *packerStackElement
}

type dcPacker struct {
	mode packerMode

	packData   packData
	unpackData []byte
	unpackP    int

	root packerInterface

	stack *packerStackElement

	currentField      packerInterface
	currentParent     packerInterface
	currentFieldIndex int

	pushMarker int

	popMarker       int
	numNestedFields int
	lastSwitch      *switchParameter

	parseError bool
	packError  bool
	rangeError bool
}

func newDCPacker() *dcPacker {
	return &dcPacker{mode: modeIdle}
}

func (p *dcPacker) clearData() {
	p.packData.clear()
	p.unpackData = nil
}

func (p *dcPacker) setUnpackData(data []byte) {
	p.unpackData = data
	p.unpackP = 0
}

func (p *dcPacker) beginPack(root packerInterface) {
	p.mode = modePack
	p.parseError = false
	p.packError = false
	p.rangeError = false

	p.root = root
	p.currentField = root
	p.currentParent = nil
	p.currentFieldIndex = 0
	p.numNestedFields = 0
}

func (p *dcPacker) endPack() bool {
	p.mode = modeIdle

	if p.stack != nil || p.currentField != nil || p.currentParent != nil {
		p.packError = true
	}

	p.clear()
	return !p.hadError()
}

func (p *dcPacker) beginUnpack(root packerInterface) {
	p.mode = modeUnpack
	p.parseError = false
	p.packError = false
	p.rangeError = false

	p.root = root
	p.currentField = root
	p.currentParent = nil
	p.currentFieldIndex = 0
	p.numNestedFields = 0
}

func (p *dcPacker) endUnpack() bool {
	p.mode = modeIdle

	if p.stack != nil || p.currentField != nil || p.currentParent != nil {
		p.packError = true
	}

	p.clear()
	return !p.hadError()
}

func (p *dcPacker) clear() {
	p.clearStack()
	p.currentField = nil
	p.currentParent = nil
	p.currentFieldIndex = 0
	p.numNestedFields = 0
	p.pushMarker = 0
	p.popMarker = 0
	p.lastSwitch = nil
	p.root = nil
}

func (p *dcPacker) clearStack() {
	p.stack = nil
}

func (p *dcPacker) hadPackError() bool { return p.packError }

func (p *dcPacker) hadRangeError() bool { return p.rangeError }

func (p *dcPacker) hadError() bool { return p.rangeError || p.packError || p.parseError }

func (p *dcPacker) hasNestedFields() bool {
	if p.currentField == nil {
		return false
	}
	return p.currentField.HasNestedFields()
}

func (p *dcPacker) getNumNestedFields() int { return p.numNestedFields }

func (p *dcPacker) moreNestedFields() bool {
	return p.currentField != nil && !p.packError
}

func (p *dcPacker) getCurrentParent() packerInterface { return p.currentParent }

func (p *dcPacker) getCurrentField() packerInterface { return p.currentField }

func (p *dcPacker) getLastSwitch() *switchParameter { return p.lastSwitch }

func (p *dcPacker) getPackType() DCPackType {
	if p.currentField == nil {
		return PTInvalid
	}
	return p.currentField.PackType()
}

func (p *dcPacker) getCurrentFieldName() string {
	if p.currentField == nil {
		return ""
	}
	return p.currentField.Name()
}

func (p *dcPacker) getNumUnpackedBytes() int { return p.unpackP }

func (p *dcPacker) getLength() int { return p.packData.getLength() }

func (p *dcPacker) getString() string { return p.packData.getString() }

func (p *dcPacker) getData() []byte { return p.packData.getData() }

func (p *dcPacker) takeData() []byte { return p.packData.takeData() }

func (p *dcPacker) getUnpackLength() int { return len(p.unpackData) }

func (p *dcPacker) getUnpackString() string { return string(p.unpackData) }

func (p *dcPacker) appendData(buf []byte) {
	p.packData.appendData(buf)
}

func (p *dcPacker) push() {
	if !p.hasNestedFields() {
		p.packError = true
		return
	}

	element := &packerStackElement{
		currentParent:     p.currentParent,
		currentFieldIndex: p.currentFieldIndex,
		pushMarker:        p.pushMarker,
		popMarker:         p.popMarker,
		next:              p.stack,
	}
	p.stack = element
	p.currentParent = p.currentField

	numNestedFields := p.currentParent.NumNestedFields()
	lengthBytes := p.currentParent.NumLengthBytes()

	switch p.mode {
	case modePack:

		p.pushMarker = p.packData.getLength()
		p.popMarker = 0
		p.packData.appendJunk(lengthBytes)

	case modeUnpack:

		p.pushMarker = p.unpackP
		p.popMarker = 0

		if lengthBytes != 0 {
			if p.unpackP+lengthBytes > len(p.unpackData) {
				p.packError = true
			} else {
				var length int
				if lengthBytes == 4 {
					length = int(doUnpackUint32(p.unpackData[p.unpackP:]))
					p.unpackP += 4
				} else {
					length = int(doUnpackUint16(p.unpackData[p.unpackP:]))
					p.unpackP += 2
				}
				p.popMarker = p.unpackP + length

				if length == 0 {
					numNestedFields = 0
				} else {
					numNestedFields = p.currentParent.CalcNumNestedFields(length)
				}
			}
		}

	default:
		p.packError = true
	}

	p.numNestedFields = numNestedFields
	p.currentFieldIndex = 0

	if p.numNestedFields >= 0 && p.currentFieldIndex >= p.numNestedFields {
		p.currentField = nil
	} else {
		p.currentField = p.currentParent.GetNestedField(p.currentFieldIndex)
	}
}

func (p *dcPacker) pop() {
	if p.currentField != nil && p.numNestedFields >= 0 {

		p.packError = true
	} else if p.mode == modeUnpack && p.popMarker != 0 && p.unpackP != p.popMarker {

		p.packError = true
	}

	if p.stack == nil {

		p.packError = true
	} else {
		if !p.currentParent.ValidateNumNestedFields(p.currentFieldIndex) {

			p.packError = true
		}

		if p.mode == modePack {
			lengthBytes := p.currentParent.NumLengthBytes()
			if lengthBytes != 0 {

				length := p.packData.getLength() - p.pushMarker - lengthBytes
				if lengthBytes == 4 {
					doPackUint32(p.packData.getRewritePointer(p.pushMarker, 4), uint(length))
				} else {
					validateUintLimits(uint(length), 16, &p.rangeError)
					doPackUint16(p.packData.getRewritePointer(p.pushMarker, 2), uint(length))
				}
			}
		}

		p.currentField = p.currentParent
		p.currentParent = p.stack.currentParent
		p.currentFieldIndex = p.stack.currentFieldIndex
		p.pushMarker = p.stack.pushMarker
		p.popMarker = p.stack.popMarker
		if p.currentParent == nil {
			p.numNestedFields = 0
		} else {
			p.numNestedFields = p.currentParent.NumNestedFields()
		}

		p.stack = p.stack.next
	}

	p.advance()
}

func (p *dcPacker) advance() {
	p.currentFieldIndex++
	if p.numNestedFields >= 0 && p.currentFieldIndex >= p.numNestedFields {

		p.currentField = nil

		if p.currentParent != nil {
			if sp, ok := p.currentParent.(*switchParameter); ok {
				p.handleSwitch(sp)
			}
		}
	} else if p.popMarker != 0 && p.unpackP >= p.popMarker {

		p.currentField = nil
	} else {

		p.currentField = p.currentParent.GetNestedField(p.currentFieldIndex)
	}
}

func (p *dcPacker) handleSwitch(sw *switchParameter) {

	var newParent packerInterface

	switch p.mode {
	case modePack:
		data := p.packData.getData()
		newParent = sw.applySwitch(data[p.pushMarker:])
	case modeUnpack:
		newParent = sw.applySwitch(p.unpackData[p.pushMarker:p.unpackP])
	}

	if newParent == nil {

		p.rangeError = true
		return
	}

	p.lastSwitch = sw

	p.currentParent = newParent
	p.numNestedFields = p.currentParent.NumNestedFields()

	if p.numNestedFields < 0 || p.currentFieldIndex < p.numNestedFields {
		p.currentField = p.currentParent.GetNestedField(p.currentFieldIndex)
	}
}

func (p *dcPacker) packDouble(value float64) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackDouble(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packInt(value int) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackInt(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packUint(value uint) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackUint(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packInt64(value int64) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackInt64(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packUint64(value uint64) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackUint64(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packString(value string) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackString(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packBlob(value []byte) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.currentField.PackBlob(&p.packData, value, &p.packError, &p.rangeError)
	p.advance()
}

func (p *dcPacker) packLiteralValue(value []byte) {
	if p.currentField == nil {
		p.packError = true
		return
	}
	p.packData.appendData(value)
	p.advance()
}

func (p *dcPacker) packDefaultValue() {
	if p.currentField == nil {
		p.packError = true
		return
	}
	if p.currentField.PackDefaultValue(&p.packData, &p.packError) {
		p.advance()
	} else {

		p.push()
		for p.moreNestedFields() {
			p.packDefaultValue()
		}
		p.pop()
	}
}

func (p *dcPacker) unpackDouble() float64 {
	var value float64
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackDouble(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackInt() int {
	var value int
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackInt(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackUint() uint {
	var value uint
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackUint(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackInt64() int64 {
	var value int64
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackInt64(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackUint64() uint64 {
	var value uint64
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackUint64(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackString() string {
	var value string
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackString(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackBlob() []byte {
	var value []byte
	if p.currentField == nil {
		p.packError = true
		return value
	}
	p.currentField.UnpackBlob(p.unpackData, &p.unpackP, &value, &p.packError, &p.rangeError)
	p.advance()
	return value
}

func (p *dcPacker) unpackLiteralValue() []byte {
	start := p.unpackP
	p.unpackSkip()
	return append([]byte(nil), p.unpackData[start:p.unpackP]...)
}

func (p *dcPacker) unpackValidate() {
	if p.currentField == nil {
		p.packError = true
		return
	}
	if p.currentField.UnpackValidate(p.unpackData, &p.unpackP, &p.packError, &p.rangeError) {
		p.advance()
	} else {

		p.push()
		for p.moreNestedFields() {
			p.unpackValidate()
		}
		p.pop()
	}
}

func (p *dcPacker) unpackSkip() {
	if p.currentField == nil {
		p.packError = true
		return
	}
	if p.currentField.UnpackSkip(p.unpackData, &p.unpackP, &p.packError) {
		p.advance()
	} else {

		p.push()
		for p.moreNestedFields() {
			p.unpackSkip()
		}
		p.pop()
	}
}
