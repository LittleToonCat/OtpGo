package dc

import "os"

var defaultKeywordDefs = []struct {
	name string
	flag int32
}{
	{"required", 0x0001},
	{"broadcast", 0x0002},
	{"ownrecv", 0x0004},
	{"ram", 0x0008},
	{"db", 0x0010},
	{"clsend", 0x0020},
	{"clrecv", 0x0040},
	{"ownsend", 0x0080},
	{"airecv", 0x0100},
}

type dcFile struct {
	classes []*dcClass

	thingsByName map[string]interface{}

	imports []dcImport

	typedefs       []*dcTypedef
	typedefsByName map[string]*dcTypedef

	keywords        *keywordList
	defaultKeywords *keywordList

	fieldsByIndex []field

	allObjectsValid      bool
	inheritedFieldsStale bool

	multipleInheritance   bool
	virtualInheritance    bool
	sortInheritanceByFile bool

	errorsFromLastParse []error
}

type dcImport struct {
	module  string
	symbols []string
}

func newDCFile() *dcFile {
	f := &dcFile{
		thingsByName:          make(map[string]interface{}),
		typedefsByName:        make(map[string]*dcTypedef),
		allObjectsValid:       true,
		multipleInheritance:   true,
		virtualInheritance:    true,
		sortInheritanceByFile: true,
	}
	f.setupDefaultKeywords()
	return f
}

func (f *dcFile) SetMultipleInheritance(enable bool) { f.multipleInheritance = enable }

func (f *dcFile) SetVirtualInheritance(enable bool) { f.virtualInheritance = true }

func (f *dcFile) SetSortInheritanceByFile(enable bool) { f.sortInheritanceByFile = enable }

func (f *dcFile) GetMultipleInheritance() bool   { return f.multipleInheritance }
func (f *dcFile) GetVirtualInheritance() bool    { return f.virtualInheritance }
func (f *dcFile) GetSortInheritanceByFile() bool { return f.sortInheritanceByFile }

func (f *dcFile) Clear() {
	f.classes = nil
	f.imports = nil
	f.thingsByName = make(map[string]interface{})
	f.typedefs = nil
	f.typedefsByName = make(map[string]*dcTypedef)
	f.keywords = nil
	f.setupDefaultKeywords()

	f.allObjectsValid = true
	f.inheritedFieldsStale = false
}

func (f *dcFile) Read(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return f.ReadString(data, path)
}

func (f *dcFile) ReadString(src []byte, filename string) bool {
	p := newParser(src, f)
	errs := p.parseDCFile()
	f.errorsFromLastParse = errs
	f.primeCaches()
	return len(errs) == 0
}

func (f *dcFile) LastParseErrors() []error {
	return f.errorsFromLastParse
}

func (f *dcFile) primeCaches() {
	for _, c := range f.classes {
		n := c.GetNumInheritedFields()
		for i := 0; i < n; i++ {
			c.GetInheritedField(i).GetDefaultValue()
		}
		if c.HasConstructor() {
			c.GetConstructor().GetDefaultValue()
		}
	}
}

func (f *dcFile) GetNumClasses() int { return len(f.classes) }

func (f *dcFile) GetClass(n int) *dcClass { return f.classes[n] }

func (f *dcFile) GetClassByName(name string) *dcClass {
	if v, ok := f.thingsByName[name]; ok {
		if c, ok := v.(*dcClass); ok {
			return c
		}
	}
	return nil
}

func (f *dcFile) GetSwitchByName(name string) *dcSwitch {
	if v, ok := f.thingsByName[name]; ok {
		if s, ok := v.(*dcSwitch); ok {
			return s
		}
	}
	return nil
}

func (f *dcFile) GetFieldByIndex(indexNumber int) field {
	if !f.GetMultipleInheritance() {
		panic("dcFile.GetFieldByIndex: only valid when MultipleInheritance is enabled")
	}
	if indexNumber >= 0 && indexNumber < len(f.fieldsByIndex) {
		return f.fieldsByIndex[indexNumber]
	}
	return nil
}

func (f *dcFile) GetNumImportModules() int { return len(f.imports) }

func (f *dcFile) GetImportModule(n int) string { return f.imports[n].module }

func (f *dcFile) GetNumImportSymbols(n int) int { return len(f.imports[n].symbols) }

func (f *dcFile) GetImportSymbol(n, i int) string { return f.imports[n].symbols[i] }

func (f *dcFile) GetNumTypedefs() int { return len(f.typedefs) }

func (f *dcFile) GetTypedef(n int) *dcTypedef { return f.typedefs[n] }

func (f *dcFile) GetTypedefByName(name string) *dcTypedef { return f.typedefsByName[name] }

func (f *dcFile) GetNumKeywords() int { return f.keywords.GetNumKeywords() }

func (f *dcFile) GetKeyword(n int) *keyword { return f.keywords.GetKeyword(n) }

func (f *dcFile) GetKeywordByName(name string) *keyword {
	kw := f.keywords.GetKeywordByName(name)
	if kw == nil {
		kw = f.defaultKeywords.GetKeywordByName(name)
		if kw != nil {

			f.keywords.AddKeyword(kw)
		}
	}
	return kw
}

func (f *dcFile) AllObjectsValid() bool { return f.allObjectsValid }

func (f *dcFile) GetHash() uint32 {
	h := newHashGenerator()
	f.GenerateHash(h)
	return h.getHash()
}

func (f *dcFile) GenerateHash(hashgen *hashGenerator) {
	if f.GetVirtualInheritance() {

		if f.GetSortInheritanceByFile() {
			hashgen.addInt(1)
		} else {
			hashgen.addInt(2)
		}
	}

	hashgen.addInt(int32(len(f.classes)))
	for _, c := range f.classes {
		c.GenerateHash(hashgen)
	}
}

func (f *dcFile) AddClass(dclass *dcClass) bool {
	if dclass.GetName() != "" {
		if _, exists := f.thingsByName[dclass.GetName()]; exists {
			return false
		}
		f.thingsByName[dclass.GetName()] = dclass
	}

	if !dclass.IsStruct() {
		dclass.SetNumber(f.GetNumClasses())
	}
	f.classes = append(f.classes, dclass)

	if dclass.IsBogusClass() {
		f.allObjectsValid = false
	}

	return true
}

func (f *dcFile) AddSwitch(dswitch *dcSwitch) bool {
	if dswitch.GetName() != "" {
		if _, exists := f.thingsByName[dswitch.GetName()]; exists {
			return false
		}
		f.thingsByName[dswitch.GetName()] = dswitch
	}
	return true
}

func (f *dcFile) AddImportModule(module string) {
	f.imports = append(f.imports, dcImport{module: module})
}

func (f *dcFile) AddImportSymbol(symbol string) {
	f.imports[len(f.imports)-1].symbols = append(f.imports[len(f.imports)-1].symbols, symbol)
}

func (f *dcFile) AddTypedef(dtypedef *dcTypedef) bool {
	if _, exists := f.typedefsByName[dtypedef.GetName()]; exists {
		return false
	}
	f.typedefsByName[dtypedef.GetName()] = dtypedef
	dtypedef.SetNumber(f.GetNumTypedefs())
	f.typedefs = append(f.typedefs, dtypedef)

	if dtypedef.IsBogusTypedef() {
		f.allObjectsValid = false
	}

	return true
}

func (f *dcFile) AddKeyword(name string) bool {
	kw := newCustomKeyword(name)
	return f.keywords.AddKeyword(kw)
}

func (f *dcFile) setNewIndexNumber(fld field) {
	fld.SetNumber(len(f.fieldsByIndex))
	f.fieldsByIndex = append(f.fieldsByIndex, fld)
}

func (f *dcFile) checkInheritedFields() {
	if f.inheritedFieldsStale {
		f.rebuildInheritedFields()
	}
}

func (f *dcFile) markInheritedFieldsStale() {
	f.inheritedFieldsStale = true
}

func (f *dcFile) setupDefaultKeywords() {
	f.keywords = newKeywordList()
	f.defaultKeywords = newKeywordList()
	for _, def := range defaultKeywordDefs {
		f.defaultKeywords.AddKeyword(newKeyword(def.name, def.flag))
	}
}

func (f *dcFile) rebuildInheritedFields() {
	f.inheritedFieldsStale = false
	for _, c := range f.classes {
		c.clearInheritedFields()
	}
	for _, c := range f.classes {
		c.rebuildInheritedFields()
	}
}
