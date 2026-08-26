package dc

func (p *dcPacker) rawPackUint16(value uint) {
	doPackUint16(p.packData.getWritePointer(2), value)
}

func (p *dcPacker) rawUnpackUint16() uint {
	if p.unpackP+2 > len(p.unpackData) {
		p.packError = true
		return 0
	}
	value := doUnpackUint16(p.unpackData[p.unpackP:])
	p.unpackP += 2
	return value
}
