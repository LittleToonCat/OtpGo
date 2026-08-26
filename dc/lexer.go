package dc

import (
	"fmt"
	"strconv"
	"strings"
)

type tokenType int

const (
	tokEOF tokenType = iota
	tokKwDclass
	tokKwStruct
	tokKwFrom
	tokKwImport
	tokKwKeyword
	tokKwTypedef
	tokKwSwitch
	tokKwCase
	tokKwDefault
	tokKwBreak
	tokKwInt8
	tokKwInt16
	tokKwInt32
	tokKwInt64
	tokKwUint8
	tokKwUint16
	tokKwUint32
	tokKwUint64
	tokKwFloat64
	tokKwString
	tokKwBlob
	tokKwBlob32
	tokKwInt8Array
	tokKwInt16Array
	tokKwInt32Array
	tokKwUint8Array
	tokKwUint16Array
	tokKwUint32Array
	tokKwUint32Uint8Array
	tokKwChar
	tokUnsignedInteger
	tokSignedInteger
	tokReal
	tokString
	tokHexString
	tokIdentifier
	tokKeyword
	tokChar
)

var fixedKeywords = map[string]tokenType{
	"dclass":           tokKwDclass,
	"struct":           tokKwStruct,
	"from":             tokKwFrom,
	"import":           tokKwImport,
	"keyword":          tokKwKeyword,
	"typedef":          tokKwTypedef,
	"switch":           tokKwSwitch,
	"case":             tokKwCase,
	"default":          tokKwDefault,
	"break":            tokKwBreak,
	"int8":             tokKwInt8,
	"int16":            tokKwInt16,
	"int32":            tokKwInt32,
	"int64":            tokKwInt64,
	"uint8":            tokKwUint8,
	"uint16":           tokKwUint16,
	"uint32":           tokKwUint32,
	"uint64":           tokKwUint64,
	"float64":          tokKwFloat64,
	"string":           tokKwString,
	"blob":             tokKwBlob,
	"blob32":           tokKwBlob32,
	"int8array":        tokKwInt8Array,
	"int16array":       tokKwInt16Array,
	"int32array":       tokKwInt32Array,
	"uint8array":       tokKwUint8Array,
	"uint16array":      tokKwUint16Array,
	"uint32array":      tokKwUint32Array,
	"uint32uint8array": tokKwUint32Uint8Array,
	"char":             tokKwChar,
}

type token struct {
	typ      tokenType
	str      string
	uintVal  uint64
	intVal   int64
	realVal  float64
	bytesVal []byte
	kw       *keyword
	ch       byte
	line     int
	col      int
}

type lexError struct {
	line, col int
	msg       string
}

func (e *lexError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.line, e.col, e.msg)
}

type lexer struct {
	src    []byte
	pos    int
	line   int
	col    int
	dcFile *dcFile
	errors []error
}

func newLexer(src []byte, f *dcFile) *lexer {
	return &lexer{src: src, line: 1, col: 1, dcFile: f}
}

func (lx *lexer) errorf(line, col int, format string, args ...interface{}) {
	lx.errors = append(lx.errors, &lexError{line: line, col: col, msg: fmt.Sprintf(format, args...)})
}

func (lx *lexer) peekByte() byte {
	if lx.pos >= len(lx.src) {
		return 0
	}
	return lx.src[lx.pos]
}

func (lx *lexer) peekByteAt(offset int) byte {
	if lx.pos+offset >= len(lx.src) {
		return 0
	}
	return lx.src[lx.pos+offset]
}

func (lx *lexer) advance() byte {
	c := lx.src[lx.pos]
	lx.pos++
	if c == '\n' {
		lx.line++
		lx.col = 1
	} else {
		lx.col++
	}
	return c
}

func (lx *lexer) atEOF() bool { return lx.pos >= len(lx.src) }

func isDigit(c byte) bool    { return c >= '0' && c <= '9' }
func isHexDigit(c byte) bool { return isDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F') }
func isAlpha(c byte) bool    { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' }
func isAlnum(c byte) bool    { return isAlpha(c) || isDigit(c) }

func (lx *lexer) next() token {
	for {
		if lx.atEOF() {
			return token{typ: tokEOF, line: lx.line, col: lx.col}
		}

		c := lx.peekByte()

		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			lx.advance()
			continue
		}

		if c == '/' && lx.peekByteAt(1) == '/' {
			for !lx.atEOF() && lx.peekByte() != '\n' {
				lx.advance()
			}
			continue
		}
		if c == '/' && lx.peekByteAt(1) == '*' {
			startLine, startCol := lx.line, lx.col
			lx.advance()
			lx.advance()
			lastC := byte(0)
			closed := false
			for !lx.atEOF() {
				cur := lx.peekByte()
				if lastC == '*' && cur == '/' {
					lx.advance()
					closed = true
					break
				}
				if lastC == '/' && cur == '*' {
					lx.errorf(lx.line, lx.col, "This comment contains a nested /* symbol--possibly unclosed?")
				}
				lastC = lx.advance()
			}
			if !closed {
				lx.errorf(startLine, startCol, "This comment marker is unclosed.")
			}
			continue
		}

		startLine, startCol := lx.line, lx.col

		if isAlpha(c) {
			start := lx.pos
			for !lx.atEOF() && isAlnum(lx.peekByte()) {
				lx.advance()
			}
			text := string(lx.src[start:lx.pos])
			if tt, ok := fixedKeywords[text]; ok {
				return token{typ: tt, str: text, line: startLine, col: startCol}
			}
			if lx.dcFile != nil {
				if kw := lx.dcFile.GetKeywordByName(text); kw != nil {
					return token{typ: tokKeyword, str: text, kw: kw, line: startLine, col: startCol}
				}
			}
			return token{typ: tokIdentifier, str: text, line: startLine, col: startCol}
		}

		if isDigit(c) {
			return lx.lexNumber(startLine, startCol)
		}
		if c == '.' && isDigit(lx.peekByteAt(1)) {
			return lx.lexNumber(startLine, startCol)
		}
		if (c == '+' || c == '-') && isDigit(lx.peekByteAt(1)) {
			return lx.lexNumber(startLine, startCol)
		}
		if (c == '+' || c == '-') && lx.peekByteAt(1) == '.' && isDigit(lx.peekByteAt(2)) {
			return lx.lexNumber(startLine, startCol)
		}

		if c == '"' || c == '\'' {
			lx.advance()
			s, ok := lx.scanQuotedString(c)
			if !ok {
				lx.errorf(startLine, startCol, "This quotation mark is unterminated.")
			}
			return token{typ: tokString, str: s, line: startLine, col: startCol}
		}

		if c == '<' {
			lx.advance()
			b, ok := lx.scanHexString()
			if !ok {
				return token{typ: tokHexString, bytesVal: nil, line: startLine, col: startCol}
			}
			return token{typ: tokHexString, bytesVal: b, line: startLine, col: startCol}
		}

		lx.advance()
		return token{typ: tokChar, ch: c, line: startLine, col: startCol}
	}
}

func (lx *lexer) lexNumber(startLine, startCol int) token {
	start := lx.pos

	neg := false
	hadSign := false
	if lx.peekByte() == '+' || lx.peekByte() == '-' {
		neg = lx.peekByte() == '-'
		hadSign = true
		lx.advance()
	}

	if !hadSign && lx.peekByte() == '0' && lx.peekByteAt(1) == 'x' {
		lx.advance()
		lx.advance()
		hexStart := lx.pos
		for isHexDigit(lx.peekByte()) {
			lx.advance()
		}
		text := string(lx.src[start:lx.pos])
		var value uint64
		for _, c := range lx.src[hexStart:lx.pos] {
			next := value*16 + uint64(hexDigitValue(c))
			if next < value {
				lx.errorf(startLine, startCol, "Number out of range.")
				return token{typ: tokUnsignedInteger, str: text, uintVal: 1, line: startLine, col: startCol}
			}
			value = next
		}
		return token{typ: tokUnsignedInteger, str: text, uintVal: value, line: startLine, col: startCol}
	}

	for isDigit(lx.peekByte()) {
		lx.advance()
	}
	isReal := false
	if lx.peekByte() == '.' {

		lx.advance()
		for isDigit(lx.peekByte()) {
			lx.advance()
		}
		isReal = true
	}
	if isReal && (lx.peekByte() == 'e' || lx.peekByte() == 'E') {
		save := lx.pos
		lx.advance()
		if lx.peekByte() == '+' || lx.peekByte() == '-' {
			lx.advance()
		}
		expStart := lx.pos
		for isDigit(lx.peekByte()) {
			lx.advance()
		}
		if lx.pos == expStart {

			lx.pos = save
		}
	}

	text := string(lx.src[start:lx.pos])

	if isReal {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			lx.errorf(startLine, startCol, "invalid real number %q", text)
		}
		return token{typ: tokReal, str: text, realVal: f, line: startLine, col: startCol}
	}

	digits := text
	if neg {
		digits = strings.TrimPrefix(text, "-")
	} else if strings.HasPrefix(text, "+") {
		digits = strings.TrimPrefix(text, "+")
	}

	if neg || strings.HasPrefix(text, "+") {

		var value uint64
		overflow := false
		for i := 0; i < len(digits); i++ {
			next := value*10 + uint64(digits[i]-'0')
			if next < value {
				overflow = true
				break
			}
			value = next
		}
		var result int64
		if neg {
			result = -int64(value)
			if !overflow && result > 0 {
				overflow = true
			}
		} else {
			result = int64(value)
			if !overflow && result < 0 {
				overflow = true
			}
		}
		if overflow {
			lx.errorf(startLine, startCol, "Number out of range.")
			result = 1
		}
		return token{typ: tokSignedInteger, str: text, intVal: result, line: startLine, col: startCol}
	}

	var value uint64
	for i := 0; i < len(digits); i++ {
		next := value*10 + uint64(digits[i]-'0')
		if next < value {
			lx.errorf(startLine, startCol, "Number out of range.")
			return token{typ: tokUnsignedInteger, str: text, uintVal: 1, line: startLine, col: startCol}
		}
		value = next
	}
	return token{typ: tokUnsignedInteger, str: text, uintVal: value, line: startLine, col: startCol}
}

func hexDigitValue(c byte) int {
	switch {
	case c >= '0' && c <= '9':
		return int(c - '0')
	case c >= 'a' && c <= 'f':
		return int(c-'a') + 10
	case c >= 'A' && c <= 'F':
		return int(c-'A') + 10
	}
	return 0
}

func (lx *lexer) scanQuotedString(quote byte) (string, bool) {
	var result []byte
	for {
		if lx.atEOF() {
			return string(result), false
		}
		c := lx.peekByte()
		if c == quote {
			lx.advance()
			return string(result), true
		}
		if c == '\n' {
			return string(result), false
		}
		if c == '\\' {
			lx.advance()
			if lx.atEOF() {
				return string(result), false
			}
			e := lx.peekByte()
			switch e {
			case 'a':
				result = append(result, '\a')
				lx.advance()
			case 'n':
				result = append(result, '\n')
				lx.advance()
			case 'r':
				result = append(result, '\r')
				lx.advance()
			case 't':
				result = append(result, '\t')
				lx.advance()
			case 'x':
				lx.advance()
				hex := 0
				for i := 0; i < 2 && isHexDigit(lx.peekByte()); i++ {
					hex = hex*16 + hexDigitValue(lx.peekByte())
					lx.advance()
				}
				result = append(result, byte(hex))
			case '0':
				lx.advance()
				oct := 0
				for i := 0; i < 3 && lx.peekByte() >= '0' && lx.peekByte() < '7'; i++ {
					oct = oct*8 + int(lx.peekByte()-'0')
					lx.advance()
				}
				result = append(result, byte(oct))
			case '1', '2', '3', '4', '5', '6', '7', '8', '9':
				dec := 0
				for i := 0; i < 3 && isDigit(lx.peekByte()); i++ {
					dec = dec*10 + int(lx.peekByte()-'0')
					lx.advance()
				}
				result = append(result, byte(dec))
			default:
				result = append(result, e)
				lx.advance()
			}
			continue
		}
		result = append(result, c)
		lx.advance()
	}
}

func (lx *lexer) scanHexString() ([]byte, bool) {
	var result []byte
	odd := false
	last := 0
	for {
		if lx.atEOF() {
			return nil, false
		}
		c := lx.peekByte()
		if c == '>' {
			lx.advance()
			if odd {
				return nil, false
			}
			return result, true
		}
		var value int
		switch {
		case c >= '0' && c <= '9':
			value = int(c - '0')
		case c >= 'a' && c <= 'f':
			value = int(c-'a') + 10
		case c >= 'A' && c <= 'F':
			value = int(c-'A') + 10
		default:
			return nil, false
		}
		lx.advance()
		odd = !odd
		if odd {
			last = value
		} else {
			result = append(result, byte((last<<4)|value))
		}
	}
}
