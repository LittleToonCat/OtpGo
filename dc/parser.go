package dc

import "fmt"

type parseError struct {
	line, col int
	msg       string
}

func (e *parseError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.line, e.col, e.msg)
}

type parser struct {
	lx     *lexer
	tok    token
	peek   token
	dcFile *dcFile
	errors []error

	currentClass *dcClass
}

func newParser(src []byte, f *dcFile) *parser {
	p := &parser{lx: newLexer(src, f), dcFile: f}
	p.tok = p.lx.next()
	p.peek = p.lx.next()
	return p
}

func (p *parser) advance() {
	p.tok = p.peek
	p.peek = p.lx.next()
}

func (p *parser) errorf(format string, args ...interface{}) {
	p.errors = append(p.errors, &parseError{line: p.tok.line, col: p.tok.col, msg: fmt.Sprintf(format, args...)})
}

func (p *parser) isChar(c byte) bool { return p.tok.typ == tokChar && p.tok.ch == c }

func (p *parser) expectChar(c byte) bool {
	if !p.isChar(c) {
		p.errorf("expected %q", c)
		return false
	}
	p.advance()
	return true
}

func (p *parser) parseDCFile() []error {
	for p.tok.typ != tokEOF {
		p.parseDeclaration()
	}
	return append(append([]error(nil), p.lx.errors...), p.errors...)
}

func (p *parser) parseDeclaration() {
	switch p.tok.typ {
	case tokKwDclass, tokKwStruct:
		p.parseClassOrStruct()
	case tokKwSwitch:
		sw := p.parseSwitchDecl()
		if sw != nil {
			if !p.dcFile.AddSwitch(sw) {
				p.errorf("Duplicate switch name: %s", sw.GetName())
			}
		}
	case tokKwImport:
		p.parseImport()
	case tokKwFrom:
		p.parseFromImport()
	case tokKwTypedef:
		p.parseTypedefDecl()
	case tokKwKeyword:
		p.parseKeywordDecl()
	case tokChar:
		if p.tok.ch == ';' {
			p.advance()
			return
		}
		p.errorf("unexpected character %q", p.tok.ch)
		p.advance()
	default:
		p.errorf("unexpected token")
		p.advance()
	}
}

func (p *parser) parseImport() {
	p.advance()
	mod := p.parseSlashIdent()
	for p.isChar('.') {
		p.advance()
		mod += "." + p.parseSlashIdent()
	}
	p.dcFile.AddImportModule(mod)
}

func (p *parser) parseFromImport() {
	p.advance()
	mod := p.parseSlashIdent()
	for p.isChar('.') {
		p.advance()
		mod += "." + p.parseSlashIdent()
	}
	p.dcFile.AddImportModule(mod)
	if p.tok.typ != tokKwImport {
		p.errorf("expected 'import'")
		return
	}
	p.advance()
	if p.isChar('*') {
		p.advance()
		return
	}
	for {
		sym := p.parseSlashIdent()
		p.dcFile.AddImportSymbol(sym)
		if p.isChar(',') {
			p.advance()
			continue
		}
		break
	}
}

func (p *parser) parseSlashIdent() string {
	if p.tok.typ != tokIdentifier {
		p.errorf("expected identifier")
		return ""
	}
	s := p.tok.str
	p.advance()
	for p.isChar('/') {
		p.advance()
		if p.tok.typ != tokIdentifier {
			p.errorf("expected identifier")
			break
		}
		s += "/" + p.tok.str
		p.advance()
	}
	return s
}

func (p *parser) parseKeywordDecl() {
	p.advance()
	for p.tok.typ == tokIdentifier || p.tok.typ == tokKeyword {
		if p.tok.typ == tokKeyword {

			p.tok.kw.ClearHistoricalFlag()
			p.advance()
		} else {
			name := p.tok.str
			p.advance()
			p.dcFile.AddKeyword(name)
		}
	}
	p.expectChar(';')
}

func (p *parser) parseKeywordList() []*keyword {
	var kws []*keyword
	for p.tok.typ == tokKeyword {
		kws = append(kws, p.tok.kw)
		p.advance()
	}
	return kws
}

func attachKeywords(pi packerInterface, kws []*keyword) {
	fb, ok := pi.(interface{ AddKeyword(*keyword) bool })
	if !ok {
		return
	}
	for _, kw := range kws {
		fb.AddKeyword(kw)
	}
}

func (p *parser) parseTypedefDecl() {
	p.advance()
	param := p.parseParameter()
	if param == nil {
		p.skipToSemicolon()
		return
	}
	p.expectChar(';')

	td := newDCTypedef(param, false)
	if !p.dcFile.AddTypedef(td) {
		existing := p.dcFile.GetTypedefByName(td.GetName())
		if existing != nil && existing.IsBogusTypedef() {
			p.errorf("typedef defined after its first reference: %s", td.GetName())
		} else {
			p.errorf("Duplicate typedef name: %s", td.GetName())
		}
	}
}

func (p *parser) skipToSemicolon() {
	for !p.isChar(';') && p.tok.typ != tokEOF {
		p.advance()
	}
	if p.isChar(';') {
		p.advance()
	}
}

func (p *parser) parseClassOrStruct() {
	isStruct := p.tok.typ == tokKwStruct
	p.advance()

	name := ""
	if p.tok.typ == tokIdentifier {
		name = p.tok.str
		p.advance()
	}

	dclass := newDCClass(p.dcFile, name, isStruct, false)

	if p.isChar(':') {
		p.advance()
		for {
			if p.tok.typ != tokIdentifier {
				p.errorf("expected base class name")
				break
			}
			baseName := p.tok.str
			p.advance()

			var base *dcClass
			if isStruct {
				base = p.resolveStructName(baseName)
			} else {
				base = p.resolveClassName(baseName)
			}
			if base != nil {
				if !isStruct && len(dclass.parents) > 0 && !p.dcFile.GetMultipleInheritance() {
					p.errorf("Multiple inheritance is not supported in this .dc file.")
				} else {
					dclass.AddParent(base)
				}
			}

			if p.isChar(',') {
				p.advance()
				continue
			}
			break
		}
	}

	p.expectChar('{')

	prevClass := p.currentClass
	p.currentClass = dclass
	for !p.isChar('}') && p.tok.typ != tokEOF {
		if p.isChar(';') {
			p.advance()
			continue
		}
		fld := p.parseClassField(isStruct)
		if fld == nil {

		} else if !dclass.AddField(fld) {
			p.errorf("Duplicate field name: %s", fld.Name())
		} else if !isStruct && fld.GetNumber() < 0 {
			p.errorf("A non-network field cannot be stored on a dclass")
		}
		if p.isChar(';') {
			p.advance()
		} else if !p.isChar('}') {
			p.errorf("expected ';'")
			p.skipToSemicolon()
		}
	}
	p.currentClass = prevClass
	p.expectChar('}')

	if !p.dcFile.AddClass(dclass) {
		existing := p.dcFile.GetClassByName(name)
		if existing != nil && existing.IsBogusClass() {
			p.errorf("Base class defined after its first reference: %s", name)
		} else {
			p.errorf("Duplicate class name: %s", name)
		}
	}
}

func (p *parser) resolveClassName(name string) *dcClass {
	existing := p.dcFile.GetClassByName(name)
	if existing == nil {
		bogus := newDCClass(p.dcFile, name, false, true)
		p.dcFile.AddClass(bogus)
		return bogus
	}
	if existing.IsStruct() {
		p.errorf("struct name not allowed: %s", name)
		return nil
	}
	return existing
}

func (p *parser) resolveStructName(name string) *dcClass {
	existing := p.dcFile.GetClassByName(name)
	if existing == nil {
		bogus := newDCClass(p.dcFile, name, false, true)
		p.dcFile.AddClass(bogus)
		return bogus
	}
	if !existing.IsStruct() {
		p.errorf("struct name required: %s", name)
		return nil
	}
	return existing
}

func (p *parser) parseClassField(isStruct bool) field {
	if p.isChar('(') || (p.tok.typ == tokIdentifier && p.peek.typ == tokChar && p.peek.ch == '(') {
		return p.parseAtomicField(isStruct)
	}
	if p.tok.typ == tokIdentifier && p.peek.typ == tokChar && p.peek.ch == ':' {
		return p.parseMolecularField(isStruct)
	}
	return p.parseParameterField(isStruct)
}

func (p *parser) parseAtomicField(isStruct bool) field {
	name := ""
	if p.tok.typ == tokIdentifier {
		name = p.tok.str
		p.advance()
	}
	if !p.expectChar('(') {
		return nil
	}

	af := newAtomicField(name, p.currentClass, false)
	if !p.isChar(')') {
		for {
			param := p.parseParameterWithDefault()
			if param != nil {
				af.AddElement(param)
			}
			if p.isChar(',') {
				p.advance()
				continue
			}
			break
		}
	}
	p.expectChar(')')

	kws := p.parseKeywordList()
	if isStruct {
		if len(kws) > 0 {
			p.errorf("Communication keywords are not allowed here.")
		}
	} else {
		attachKeywords(af, kws)
	}
	return af
}

func (p *parser) parseMolecularField(isStruct bool) field {
	name := p.tok.str
	p.advance()
	p.expectChar(':')

	mf := newMolecularField(name, p.currentClass)
	for {
		if p.tok.typ != tokIdentifier {
			p.errorf("expected atomic field name")
			break
		}
		atomicName := p.tok.str
		p.advance()

		var atomic *atomicField
		if p.currentClass != nil {
			resolved := p.currentClass.GetFieldByName(atomicName)
			if resolved == nil {

				if p.currentClass.InheritsFromBogusClass() {
					atomic = newAtomicField(atomicName, p.currentClass, true)
				} else {

					p.errorf("Unknown field: %s", atomicName)
				}
			} else if af, ok := resolved.(*atomicField); ok {
				atomic = af
			} else {
				p.errorf("Not an atomic field: %s", atomicName)
			}
		}

		if atomic != nil {
			if mf.GetNumAtomics() > 0 {
				first := mf.GetAtomic(0)
				if !first.keywordList.CompareKeywords(&atomic.keywordList) {
					p.errorf("Mismatched keywords in molecule between %s and %s", first.Name(), atomic.Name())
				}
			}
			mf.AddAtomic(atomic)
		}

		if p.isChar(',') {
			p.advance()
			continue
		}
		break
	}

	kws := p.parseKeywordList()
	if len(kws) > 0 {
		p.errorf("Communication keywords are not allowed here.")
	}
	return mf
}

func (p *parser) parseParameterField(isStruct bool) field {
	param := p.parseParameterWithDefault()
	if param == nil {
		return nil
	}
	if param.Name() == "" && !isStruct {
		p.errorf("Unnamed parameters are not allowed on a dclass")
	}

	kws := p.parseKeywordList()
	if isStruct {
		if len(kws) > 0 {
			p.errorf("Communication keywords are not allowed here.")
		}
	} else {
		attachKeywords(param, kws)
	}

	fld, ok := param.(field)
	if !ok {
		p.errorf("internal error: parameter does not implement field")
		return nil
	}
	return fld
}

func (p *parser) parseParameterWithDefault() packerInterface {
	param := p.parseParameter()
	if param == nil {
		return nil
	}
	if p.isChar('=') {
		p.advance()
		p.parseDefaultValueInto(param)
	}
	return param
}

func (p *parser) parseParameter() packerInterface {
	typ := p.parseTypeDefinition()
	if typ == nil {
		return nil
	}

	if p.tok.typ == tokIdentifier {
		name := p.tok.str
		p.advance()
		typ.SetName(name)

		for {
			if p.isChar('/') {
				p.advance()
				n := p.parseSmallUnsignedInt()
				p.applyPostNameDivisor(typ, n)
			} else if p.isChar('%') {
				p.advance()
				n := p.parseNumber()
				p.applyPostNameModulus(typ, n)
			} else if p.isChar('[') {
				p.advance()
				rng := p.parseUIntRange()
				p.expectChar(']')
				typ = appendArraySpec(typ, rng)
			} else {
				break
			}
		}
	}

	return typ
}

func (p *parser) applyPostNameDivisor(typ packerInterface, n uint32) {
	if td, ok := typ.(interface{ IsFromTypedef() bool }); ok && td.IsFromTypedef() {
		p.errorf("A divisor/modulus is only allowed on a primitive type")
		return
	}
	sp, ok := typ.(*simpleParameter)
	if !ok || !sp.IsNumericType() || !sp.SetDivisor(n) {
		p.errorf("A divisor/modulus is only allowed on a primitive type")
	}
}

func (p *parser) applyPostNameModulus(typ packerInterface, n float64) {
	if td, ok := typ.(interface{ IsFromTypedef() bool }); ok && td.IsFromTypedef() {
		p.errorf("A divisor/modulus is only allowed on a primitive type")
		return
	}
	sp, ok := typ.(*simpleParameter)
	if !ok || !sp.IsNumericType() || !sp.SetModulus(n) {
		p.errorf("A divisor/modulus is only allowed on a primitive type")
	}
}

func (p *parser) parseTypeDefinition() packerInterface {
	typ := p.parseTypeName()
	if typ == nil {
		return nil
	}
	for p.isChar('[') {
		p.advance()
		rng := p.parseUIntRange()
		p.expectChar(']')
		typ = appendArraySpec(typ, rng)
	}
	return typ
}

var simpleTypeTokens = map[tokenType]DCSubatomicType{
	tokKwInt8:             STInt8,
	tokKwInt16:            STInt16,
	tokKwInt32:            STInt32,
	tokKwInt64:            STInt64,
	tokKwUint8:            STUint8,
	tokKwUint16:           STUint16,
	tokKwUint32:           STUint32,
	tokKwUint64:           STUint64,
	tokKwFloat64:          STFloat64,
	tokKwString:           STString,
	tokKwBlob:             STBlob,
	tokKwBlob32:           STBlob32,
	tokKwInt8Array:        STInt8array,
	tokKwInt16Array:       STInt16array,
	tokKwInt32Array:       STInt32array,
	tokKwUint8Array:       STUint8array,
	tokKwUint16Array:      STUint16array,
	tokKwUint32Array:      STUint32array,
	tokKwUint32Uint8Array: STUint32uint8array,
	tokKwChar:             STChar,
}

func (p *parser) parseTypeName() packerInterface {
	if _, ok := simpleTypeTokens[p.tok.typ]; ok {
		return p.parseSimpleTypeName()
	}
	switch p.tok.typ {
	case tokKwStruct:
		return p.parseInlineStructType()
	case tokKwSwitch:
		sw := p.parseSwitchDecl()
		if sw == nil {
			return nil
		}
		return newSwitchParameter(sw)
	case tokIdentifier:
		name := p.tok.str
		p.advance()
		return p.resolveTypeIdentifier(name)
	}
	p.errorf("expected a type")
	return nil
}

func (p *parser) parseSimpleTypeName() packerInterface {
	st := simpleTypeTokens[p.tok.typ]
	p.advance()
	sp := newSimpleParameter(st, 1)

	for {
		if p.isChar('(') {
			p.advance()
			rng := p.parseDoubleRange()
			p.expectChar(')')
			if !sp.SetRange(rng) {
				p.errorf("Inappropriate range for type")
			}
		} else if p.isChar('/') {
			p.advance()
			n := p.parseSmallUnsignedInt()
			if !sp.IsNumericType() || !sp.SetDivisor(n) {
				p.errorf("Invalid divisor")
			}
		} else if p.isChar('%') {
			p.advance()
			n := p.parseNumber()
			if !sp.IsNumericType() || !sp.SetModulus(n) {
				p.errorf("Invalid modulus")
			}
		} else {
			break
		}
	}
	return sp
}

func (p *parser) resolveTypeIdentifier(name string) packerInterface {
	if td := p.dcFile.GetTypedefByName(name); td != nil {
		return td.MakeNewParameter()
	}
	if c := p.dcFile.GetClassByName(name); c != nil {
		implicit := newDCTypedef(newClassParameter(c), true)
		p.dcFile.AddTypedef(implicit)
		return implicit.MakeNewParameter()
	}
	if sw := p.dcFile.GetSwitchByName(name); sw != nil {
		implicit := newDCTypedef(newSwitchParameter(sw), true)
		p.dcFile.AddTypedef(implicit)
		return implicit.MakeNewParameter()
	}
	bogus := newBogusDCTypedef(name)
	p.dcFile.AddTypedef(bogus)
	return bogus.MakeNewParameter()
}

func (p *parser) parseInlineStructType() packerInterface {
	p.advance()
	name := ""
	if p.tok.typ == tokIdentifier {
		name = p.tok.str
		p.advance()
	}
	dclass := newDCClass(p.dcFile, name, true, false)

	if !p.expectChar('{') {
		return nil
	}
	prevClass := p.currentClass
	p.currentClass = dclass
	for !p.isChar('}') && p.tok.typ != tokEOF {
		if p.isChar(';') {
			p.advance()
			continue
		}
		fld := p.parseClassField(true)
		if fld == nil {

		} else if !dclass.AddField(fld) {
			p.errorf("Duplicate field name: %s", fld.Name())
		}
		if p.isChar(';') {
			p.advance()
		} else if !p.isChar('}') {
			p.errorf("expected ';'")
			p.skipToSemicolon()
		}
	}
	p.currentClass = prevClass
	p.expectChar('}')

	if name != "" {
		p.dcFile.AddClass(dclass)
	}
	implicit := newDCTypedef(newClassParameter(dclass), true)
	p.dcFile.AddTypedef(implicit)
	return implicit.MakeNewParameter()
}

func (p *parser) parseUIntRange() dcUnsignedIntRange {
	var rng dcUnsignedIntRange
	if p.isChar(']') {
		return rng
	}
	for {
		min := p.parseCharOrUInt()
		max := min
		if p.isChar('-') {
			p.advance()
			max = p.parseCharOrUInt()
		} else if p.tok.typ == tokSignedInteger {
			if p.tok.intVal >= 0 {
				p.errorf("Syntax error")
			} else {
				max = uint(-p.tok.intVal)
			}
			p.advance()
		}
		if !rng.addRange(min, max) {
			p.errorf("Overlapping range")
		}
		if p.isChar(',') {
			p.advance()
			continue
		}
		break
	}
	return rng
}

func (p *parser) parseCharOrUInt() uint {
	if p.tok.typ == tokString && len(p.tok.str) == 1 {
		v := uint(p.tok.str[0])
		p.advance()
		return v
	}
	if p.tok.typ == tokUnsignedInteger {
		v := uint(p.tok.uintVal)
		p.advance()
		return v
	}
	p.errorf("expected a number")
	return 0
}

func (p *parser) parseDoubleRange() dcDoubleRange {
	var rng dcDoubleRange
	if p.isChar(')') {
		return rng
	}
	for {
		min := p.parseCharOrNumber()
		max := min
		if p.isChar('-') {
			p.advance()
			max = p.parseCharOrNumber()
		} else if p.tok.typ == tokSignedInteger || p.tok.typ == tokReal {
			nval := p.parseNumberToken()
			if nval >= 0 {
				p.errorf("Syntax error")
			} else {
				max = -nval
			}
		}
		if !rng.addRange(min, max) {
			p.errorf("Overlapping range")
		}
		if p.isChar(',') {
			p.advance()
			continue
		}
		break
	}
	return rng
}

func (p *parser) parseCharOrNumber() float64 {
	if p.tok.typ == tokString && len(p.tok.str) == 1 {
		v := float64(p.tok.str[0])
		p.advance()
		return v
	}
	return p.parseNumberToken()
}

func (p *parser) parseNumberToken() float64 {
	var v float64
	switch p.tok.typ {
	case tokUnsignedInteger:
		v = float64(p.tok.uintVal)
	case tokSignedInteger:
		v = float64(p.tok.intVal)
	case tokReal:
		v = p.tok.realVal
	default:
		p.errorf("expected a number")
		return 0
	}
	p.advance()
	return v
}

func (p *parser) parseNumber() float64 { return p.parseNumberToken() }

func (p *parser) parseSmallUnsignedInt() uint32 {
	if p.tok.typ != tokUnsignedInteger {
		p.errorf("expected an unsigned integer")
		return 1
	}
	v := p.tok.uintVal
	p.advance()
	if v > 0xffffffff {
		p.errorf("Number out of range.")
		return 1
	}
	return uint32(v)
}

func (p *parser) parseSwitchDecl() *dcSwitch {
	p.advance()
	name := ""
	if p.tok.typ == tokIdentifier {
		name = p.tok.str
		p.advance()
	}
	if !p.expectChar('(') {
		return nil
	}
	keyParam := p.parseParameter()
	if !p.expectChar(')') {
		return nil
	}
	if keyParam == nil {
		return nil
	}
	keyField, ok := keyParam.(field)
	if !ok {
		p.errorf("invalid switch key type")
		return nil
	}

	sw := newDCSwitch(name, keyField)

	if !p.expectChar('{') {
		return nil
	}
	sawCaseOrDefault := false
	for !p.isChar('}') && p.tok.typ != tokEOF {
		switch {
		case p.tok.typ == tokKwCase:
			p.advance()
			val := p.parseCaseValue(keyField)
			p.expectChar(':')
			if val != nil {
				if sw.AddCase(val) < 0 {
					p.errorf("Duplicate case value")
				}
			} else {
				sw.AddInvalidCase()
			}
			sawCaseOrDefault = true
		case p.tok.typ == tokKwDefault:
			p.advance()
			p.expectChar(':')
			if !sw.AddDefault() {
				p.errorf("Duplicate default case")
			}
			sawCaseOrDefault = true
		case p.tok.typ == tokKwBreak:
			p.advance()
			p.expectChar(';')
			sw.AddBreak()
		case p.isChar(';'):
			p.advance()
		default:
			if !sawCaseOrDefault {
				p.errorf("case declaration required before first element")
				p.skipToSemicolon()
				continue
			}
			fld := p.parseParameterField(false)
			if fld == nil {
				p.skipToSemicolon()
				continue
			}
			if !sw.AddField(fld) {
				p.errorf("Duplicate field name")
			}
			p.expectChar(';')
		}
	}
	p.expectChar('}')
	return sw
}

func (p *parser) parseCaseValue(keyField field) []byte {
	pk := newDCPacker()
	pk.beginPack(keyField)
	p.parseParameterValue(pk)
	if !pk.endPack() {
		p.errorf("Invalid value for switch parameter")
		return nil
	}
	return pk.getData()
}

func (p *parser) parseDefaultValueInto(param packerInterface) {
	pk := newDCPacker()
	pk.beginPack(param)
	p.parseParameterValue(pk)
	if !pk.endPack() {
		p.errorf("Error packing default value for %s", param.Name())
		return
	}
	if fb, ok := param.(interface{ SetDefaultValue([]byte) }); ok {
		fb.SetDefaultValue(pk.getData())
	}
}

func (p *parser) parseParameterValue(pk *dcPacker) {
	switch p.tok.typ {
	case tokSignedInteger:
		v := p.tok.intVal
		p.advance()
		n := p.parseOptionalRepeatCount()
		for i := uint32(0); i < n; i++ {
			p.packNumericDefault(pk, float64(v), true)
		}
	case tokUnsignedInteger:
		v := p.tok.uintVal
		p.advance()
		n := p.parseOptionalRepeatCount()
		for i := uint32(0); i < n; i++ {
			p.packNumericDefault(pk, float64(v), false)
		}
	case tokReal:
		v := p.tok.realVal
		p.advance()
		n := p.parseOptionalRepeatCount()
		for i := uint32(0); i < n; i++ {
			p.packNumericDefault(pk, v, true)
		}
	case tokString:
		s := p.tok.str
		p.advance()
		pk.packString(s)
	case tokHexString:
		b := p.tok.bytesVal
		p.advance()
		n := p.parseOptionalRepeatCount()
		for i := uint32(0); i < n; i++ {
			pk.packBlob(b)
		}
	case tokChar:
		switch p.tok.ch {
		case '{', '[', '(':
			closing := matchingCloseChar(p.tok.ch)
			p.advance()
			pk.push()
			for !(p.tok.typ == tokChar && p.tok.ch == closing) && p.tok.typ != tokEOF {
				p.parseParameterValue(pk)
				if p.isChar(',') {
					p.advance()
					continue
				}
				break
			}
			pk.pop()
			p.expectChar(closing)
		default:
			p.errorf("expected a value")
		}
	default:
		p.errorf("expected a value")
	}
}

func (p *parser) parseOptionalRepeatCount() uint32 {
	if !p.isChar('*') {
		return 1
	}
	p.advance()
	return p.parseSmallUnsignedInt()
}

func matchingCloseChar(open byte) byte {
	switch open {
	case '{':
		return '}'
	case '[':
		return ']'
	case '(':
		return ')'
	}
	return 0
}

func (p *parser) packNumericDefault(pk *dcPacker, v float64, signedOrReal bool) {
	switch pk.getPackType() {
	case PTInt:
		pk.packInt(int(v))
	case PTUint:
		pk.packUint(uint(v))
	case PTInt64:
		pk.packInt64(int64(v))
	case PTUint64:
		pk.packUint64(uint64(v))
	case PTDouble:
		pk.packDouble(v)
	default:
		p.errorf("unexpected numeric default value")
	}
}

func parseAndPackValue(src []byte, target packerInterface) ([]byte, bool) {
	pr := newParser(src, nil)
	pk := newDCPacker()
	pk.beginPack(target)
	pr.parseParameterValue(pk)
	packOK := pk.endPack()
	parseOK := len(pr.errors) == 0 && len(pr.lx.errors) == 0
	return pk.getData(), packOK && parseOK
}
