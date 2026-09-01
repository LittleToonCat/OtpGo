package dc

import (
	"fmt"
	"strings"
)

func (p *dcPacker) unpackAndFormat(out *strings.Builder, showFieldNames bool) {
	packType := p.getPackType()

	if showFieldNames && p.getCurrentFieldName() != "" {
		if isParameterKind(p.currentField) {
			out.WriteString(p.currentField.Name())
			out.WriteString(" = ")
		}
	}

	switch packType {
	case PTInvalid:
		out.WriteString("<invalid>")
	case PTDouble:
		fmt.Fprintf(out, "%v", p.unpackDouble())
	case PTInt:
		fmt.Fprintf(out, "%d", p.unpackInt())
	case PTUint:
		fmt.Fprintf(out, "%d", p.unpackUint())
	case PTInt64:
		fmt.Fprintf(out, "%d", p.unpackInt64())
	case PTUint64:
		fmt.Fprintf(out, "%d", p.unpackUint64())
	case PTString:
		enquoteString(out, '"', p.unpackString())
	case PTBlob:
		outputHexString(out, p.unpackBlob())
	default:
		var openCh, closeCh byte
		switch packType {
		case PTArray:
			openCh, closeCh = '[', ']'
		case PTField, PTSwitch:
			openCh, closeCh = '(', ')'
		default:
			openCh, closeCh = '{', '}'
		}
		out.WriteByte(openCh)
		p.push()
		for p.moreNestedFields() && !p.hadPackError() {
			p.unpackAndFormat(out, showFieldNames)
			if p.moreNestedFields() {
				out.WriteString(", ")
			}
		}
		p.pop()
		out.WriteByte(closeCh)
	}
}

func isParameterKind(pi packerInterface) bool {
	switch pi.(type) {
	case *simpleParameter, *arrayParameter, *classParameter, *switchParameter:
		return true
	default:
		return false
	}
}

func enquoteString(out *strings.Builder, quoteMark byte, str string) {
	out.WriteByte(quoteMark)
	for i := 0; i < len(str); i++ {
		c := str[i]
		switch {
		case c == quoteMark || c == '\\':
			out.WriteByte('\\')
			out.WriteByte(c)
		case c == '\t' || c < 0x20 || c >= 0x7f:
			fmt.Fprintf(out, "\\x%02x", c)
		default:
			out.WriteByte(c)
		}
	}
	out.WriteByte(quoteMark)
}

func outputHexString(out *strings.Builder, data []byte) {
	out.WriteByte('<')
	for _, b := range data {
		fmt.Fprintf(out, "%02x", b)
	}
	out.WriteByte('>')
}
