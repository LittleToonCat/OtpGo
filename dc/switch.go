package dc

type switchFields struct {
	packerBase

	fields              []field
	fieldsByName        map[string]field
	hasDefaultValueFlag bool
}

func newSwitchFields(name string) *switchFields {
	sf := &switchFields{
		packerBase:   newPackerBase(name),
		fieldsByName: make(map[string]field),
	}
	sf.hasNestedFields = true
	sf.numNestedFields = 0
	sf.packType = PTSwitch
	sf.hasFixedByteSize = true
	sf.fixedByteSize = 0
	sf.hasFixedStructure = true
	sf.hasRangeLimits = false
	return sf
}

func (sf *switchFields) GetNestedField(n int) packerInterface { return sf.fields[n] }

func (sf *switchFields) AddField(f field) bool {
	if f.Name() != "" {
		if _, exists := sf.fieldsByName[f.Name()]; exists {
			return false
		}
		sf.fieldsByName[f.Name()] = f
	}

	sf.fields = append(sf.fields, f)
	sf.numNestedFields = len(sf.fields)

	if sf.hasFixedByteSize {
		sf.hasFixedByteSize = f.HasFixedByteSize()
		sf.fixedByteSize += f.FixedByteSize()
	}
	if sf.hasFixedStructure {
		sf.hasFixedStructure = f.HasFixedStructure()
	}
	if !sf.hasRangeLimits {
		sf.hasRangeLimits = f.HasRangeLimits()
	}
	if !sf.hasDefaultValueFlag {
		sf.hasDefaultValueFlag = f.HasDefaultValue()
	}
	return true
}

func (sf *switchFields) doCheckMatchSwitchCase(other *switchFields) bool {
	if len(sf.fields) != len(other.fields) {
		return false
	}
	for i := range sf.fields {
		if !sf.fields[i].DoCheckMatch(other.fields[i]) {
			return false
		}
	}
	return true
}

func (sf *switchFields) CalcNumNestedFields(lengthBytes int) int {
	return defaultCalcNumNestedFields(lengthBytes)
}
func (sf *switchFields) ValidateNumNestedFields(n int) bool {
	return defaultValidateNumNestedFields(n)
}

func (sf *switchFields) PackDouble(pd *packData, value float64, packError, rangeError *bool) {
	defaultPackDouble(packError)
}
func (sf *switchFields) PackInt(pd *packData, value int, packError, rangeError *bool) {
	defaultPackInt(packError)
}
func (sf *switchFields) PackUint(pd *packData, value uint, packError, rangeError *bool) {
	defaultPackUint(packError)
}
func (sf *switchFields) PackInt64(pd *packData, value int64, packError, rangeError *bool) {
	defaultPackInt64(packError)
}
func (sf *switchFields) PackUint64(pd *packData, value uint64, packError, rangeError *bool) {
	defaultPackUint64(packError)
}
func (sf *switchFields) PackString(pd *packData, value string, packError, rangeError *bool) {
	defaultPackString(packError)
}
func (sf *switchFields) PackBlob(pd *packData, value []byte, packError, rangeError *bool) {
	defaultPackBlob(packError)
}
func (sf *switchFields) PackDefaultValue(pd *packData, packError *bool) bool {
	return defaultPackDefaultValue()
}
func (sf *switchFields) UnpackDouble(data []byte, p *int, value *float64, packError, rangeError *bool) {
	defaultUnpackDouble(packError)
}
func (sf *switchFields) UnpackInt(data []byte, p *int, value *int, packError, rangeError *bool) {
	defaultUnpackInt(packError)
}
func (sf *switchFields) UnpackUint(data []byte, p *int, value *uint, packError, rangeError *bool) {
	defaultUnpackUint(packError)
}
func (sf *switchFields) UnpackInt64(data []byte, p *int, value *int64, packError, rangeError *bool) {
	defaultUnpackInt64(packError)
}
func (sf *switchFields) UnpackUint64(data []byte, p *int, value *uint64, packError, rangeError *bool) {
	defaultUnpackUint64(packError)
}
func (sf *switchFields) UnpackString(data []byte, p *int, value *string, packError, rangeError *bool) {
	defaultUnpackString(packError)
}
func (sf *switchFields) UnpackBlob(data []byte, p *int, value *[]byte, packError, rangeError *bool) {
	defaultUnpackBlob(packError)
}
func (sf *switchFields) UnpackValidate(data []byte, p *int, packError, rangeError *bool) bool {
	return defaultUnpackValidate(&sf.packerBase, data, p, packError, rangeError)
}
func (sf *switchFields) UnpackSkip(data []byte, p *int, packError *bool) bool {
	return defaultUnpackSkip(&sf.packerBase, data, p, packError)
}

func (sf *switchFields) GenerateHash(hashgen *hashGenerator) {
	panic("switchFields.GenerateHash should never be called directly")
}

func (sf *switchFields) DoCheckMatch(other packerInterface) bool {
	return false
}

func (sf *switchFields) MakeCopy() packerInterface {
	panic("switchFields.MakeCopy should never be called")
}

type switchCase struct {
	value  []byte
	fields *switchFields
}

func (sc *switchCase) doCheckMatchSwitchCase(other *switchCase) bool {
	return sc.fields.doCheckMatchSwitchCase(other.fields)
}

type dcSwitch struct {
	name         string
	keyParameter field

	cases       []*switchCase
	defaultCase *switchFields

	caseFields   []*switchFields
	nestedFields []field

	currentFields []*switchFields
	fieldsAdded   bool

	casesByValue map[string]int
}

func newDCSwitch(name string, keyParameter field) *dcSwitch {
	return &dcSwitch{
		name:         name,
		keyParameter: keyParameter,
		casesByValue: make(map[string]int),
	}
}

func (s *dcSwitch) GetName() string        { return s.name }
func (s *dcSwitch) GetKeyParameter() field { return s.keyParameter }

func (s *dcSwitch) GetNumCases() int { return len(s.cases) }

func (s *dcSwitch) GetCaseByValue(caseValue []byte) int {
	if idx, ok := s.casesByValue[string(caseValue)]; ok {
		return idx
	}
	return -1
}

func (s *dcSwitch) GetCase(n int) *switchFields   { return s.cases[n].fields }
func (s *dcSwitch) GetDefaultCase() *switchFields { return s.defaultCase }
func (s *dcSwitch) GetValue(caseIndex int) []byte { return s.cases[caseIndex].value }

func (s *dcSwitch) GetNumFields(caseIndex int) int {
	return len(s.cases[caseIndex].fields.fields)
}
func (s *dcSwitch) GetField(caseIndex, n int) field {
	return s.cases[caseIndex].fields.fields[n]
}
func (s *dcSwitch) GetFieldByName(caseIndex int, name string) field {
	return s.cases[caseIndex].fields.fieldsByName[name]
}

func (s *dcSwitch) IsFieldValid() bool { return len(s.currentFields) != 0 }

func (s *dcSwitch) AddCase(value []byte) int {
	caseIndex := len(s.cases)
	key := string(value)
	if _, exists := s.casesByValue[key]; exists {
		s.AddInvalidCase()
		return -1
	}
	s.casesByValue[key] = caseIndex

	fields := s.startNewCase()
	s.cases = append(s.cases, &switchCase{value: value, fields: fields})
	return caseIndex
}

func (s *dcSwitch) AddInvalidCase() {
	s.startNewCase()
}

func (s *dcSwitch) AddDefault() bool {
	if s.defaultCase != nil {
		s.AddInvalidCase()
		return false
	}
	s.defaultCase = s.startNewCase()
	return true
}

func (s *dcSwitch) AddField(f field) bool {
	allOK := true
	for _, fields := range s.currentFields {
		if !fields.AddField(f) {
			allOK = false
		}
	}
	s.nestedFields = append(s.nestedFields, f)
	s.fieldsAdded = true
	return allOK
}

func (s *dcSwitch) AddBreak() {
	s.currentFields = nil
	s.fieldsAdded = false
}

func (s *dcSwitch) applySwitch(valueData []byte) *switchFields {
	if idx, ok := s.casesByValue[string(valueData)]; ok {
		return s.cases[idx].fields
	}
	if s.defaultCase != nil {
		return s.defaultCase
	}
	return nil
}

func (s *dcSwitch) GenerateHash(hashgen *hashGenerator) {
	hashgen.addString(s.name)
	s.keyParameter.GenerateHash(hashgen)

	hashgen.addInt(int32(len(s.cases)))
	for _, dcase := range s.cases {
		hashgen.addBlob(dcase.value)

		fields := dcase.fields
		hashgen.addInt(int32(len(fields.fields)))
		for _, f := range fields.fields {
			f.GenerateHash(hashgen)
		}
	}

	if s.defaultCase != nil {
		fields := s.defaultCase
		hashgen.addInt(int32(len(fields.fields)))
		for _, f := range fields.fields {
			f.GenerateHash(hashgen)
		}
	}
}

func (s *dcSwitch) PackDefaultValue(pd *packData, packError *bool) bool {
	var fields *switchFields
	p := newDCPacker()
	p.beginPack(s.keyParameter)
	if len(s.cases) > 0 {
		p.packLiteralValue(s.cases[0].value)
		fields = s.cases[0].fields
	} else {
		p.packDefaultValue()
		fields = s.defaultCase
	}
	if !p.endPack() {
		*packError = true
	}

	if fields == nil {
		*packError = true
	} else {
		for i := 1; i < len(fields.fields); i++ {
			p.beginPack(fields.fields[i])
			p.packDefaultValue()
			if !p.endPack() {
				*packError = true
			}
		}
	}

	pd.appendData(p.getData())
	return true
}

func (s *dcSwitch) DoCheckMatchSwitch(other *dcSwitch) bool {
	if !s.keyParameter.DoCheckMatch(other.keyParameter) {
		return false
	}
	if len(s.cases) != len(other.cases) {
		return false
	}
	for _, c1 := range s.cases {
		idx, ok := other.casesByValue[string(c1.value)]
		if !ok {
			return false
		}
		c2 := other.cases[idx]
		if !c1.doCheckMatchSwitchCase(c2) {
			return false
		}
	}
	return true
}

func (s *dcSwitch) startNewCase() *switchFields {
	var fields *switchFields
	if len(s.currentFields) == 0 || s.fieldsAdded {
		fields = newSwitchFields(s.name)
		fields.AddField(s.keyParameter)
		s.caseFields = append(s.caseFields, fields)
		s.currentFields = append(s.currentFields, fields)
	} else {
		fields = s.currentFields[len(s.currentFields)-1]
	}
	s.fieldsAdded = false
	return fields
}
