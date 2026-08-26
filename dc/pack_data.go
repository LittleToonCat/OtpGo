package dc

type packData struct {
	buffer []byte
}

func (d *packData) clear() {
	d.buffer = d.buffer[:0]
}

func (d *packData) appendData(buf []byte) {
	d.buffer = append(d.buffer, buf...)
}

func (d *packData) getWritePointer(size int) []byte {
	oldLen := len(d.buffer)
	d.buffer = append(d.buffer, make([]byte, size)...)
	return d.buffer[oldLen : oldLen+size]
}

func (d *packData) appendJunk(size int) {
	d.buffer = append(d.buffer, make([]byte, size)...)
}

func (d *packData) rewriteData(position int, buf []byte) {
	copy(d.buffer[position:position+len(buf)], buf)
}

func (d *packData) getRewritePointer(position, size int) []byte {
	return d.buffer[position : position+size]
}

func (d *packData) getLength() int {
	return len(d.buffer)
}

func (d *packData) getData() []byte {
	return d.buffer
}

func (d *packData) getString() string {
	return string(d.buffer)
}

func (d *packData) takeData() []byte {
	data := d.buffer
	d.buffer = nil
	return data
}
