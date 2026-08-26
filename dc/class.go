package dc

import "sort"

type dcClass struct {
	dcFile     *dcFile
	name       string
	isStruct   bool
	bogusClass bool
	number     int

	parents []*dcClass

	constructor field

	fields          []field
	inheritedFields []field
	fieldsByName    map[string]field
	fieldsByIndex   map[int]field
}

func newDCClass(f *dcFile, name string, isStruct, bogusClass bool) *dcClass {
	return &dcClass{
		dcFile:        f,
		name:          name,
		isStruct:      isStruct,
		bogusClass:    bogusClass,
		number:        -1,
		fieldsByName:  make(map[string]field),
		fieldsByIndex: make(map[int]field),
	}
}

func (c *dcClass) GetDCFile() *dcFile { return c.dcFile }

func (c *dcClass) GetName() string { return c.name }

func (c *dcClass) GetNumber() int  { return c.number }
func (c *dcClass) SetNumber(n int) { c.number = n }

func (c *dcClass) IsStruct() bool { return c.isStruct }

func (c *dcClass) IsBogusClass() bool { return c.bogusClass }

func (c *dcClass) GetNumParents() int { return len(c.parents) }

func (c *dcClass) GetParent(n int) *dcClass { return c.parents[n] }

func (c *dcClass) HasConstructor() bool { return c.constructor != nil }

func (c *dcClass) GetConstructor() field { return c.constructor }

func (c *dcClass) GetNumFields() int { return len(c.fields) }

func (c *dcClass) GetField(n int) field { return c.fields[n] }

func (c *dcClass) GetFieldByName(name string) field {
	if f, ok := c.fieldsByName[name]; ok {
		return f
	}
	for _, parent := range c.parents {
		if f := parent.GetFieldByName(name); f != nil {
			return f
		}
	}
	return nil
}

func (c *dcClass) GetFieldByIndex(indexNumber int) field {
	if f, ok := c.fieldsByIndex[indexNumber]; ok {
		return f
	}
	for _, parent := range c.parents {
		if f := parent.GetFieldByIndex(indexNumber); f != nil {
			return f
		}
	}
	return nil
}

func (c *dcClass) GetNumInheritedFields() int {
	if c.dcFile != nil && c.dcFile.GetMultipleInheritance() && c.dcFile.GetVirtualInheritance() {
		c.dcFile.checkInheritedFields()
		return len(c.inheritedFields)
	}

	numFields := c.GetNumFields()
	for _, parent := range c.parents {
		numFields += parent.GetNumInheritedFields()
	}
	return numFields
}

func (c *dcClass) GetInheritedField(n int) field {
	if c.dcFile != nil && c.dcFile.GetMultipleInheritance() && c.dcFile.GetVirtualInheritance() {
		c.dcFile.checkInheritedFields()
		return c.inheritedFields[n]
	}

	for _, parent := range c.parents {
		psize := parent.GetNumInheritedFields()
		if n < psize {
			return parent.GetInheritedField(n)
		}
		n -= psize
	}
	return c.GetField(n)
}

func (c *dcClass) InheritsFromBogusClass() bool {
	if c.IsBogusClass() {
		return true
	}
	for _, parent := range c.parents {
		if parent.InheritsFromBogusClass() {
			return true
		}
	}
	return false
}

func (c *dcClass) GenerateHash(hashgen *hashGenerator) {
	hashgen.addString(c.name)
	if c.IsStruct() {
		hashgen.addInt(1)
	}

	hashgen.addInt(int32(len(c.parents)))
	for _, parent := range c.parents {
		hashgen.addInt(int32(parent.GetNumber()))
	}

	if c.constructor != nil {
		c.constructor.GenerateHash(hashgen)
	}

	hashgen.addInt(int32(len(c.fields)))
	for _, f := range c.fields {
		f.GenerateHash(hashgen)
	}
}

func (c *dcClass) clearInheritedFields() {
	c.inheritedFields = nil
}

func (c *dcClass) rebuildInheritedFields() {
	names := make(map[string]bool)
	c.inheritedFields = nil

	for _, parent := range c.parents {
		numInherited := parent.GetNumInheritedFields()
		for i := 0; i < numInherited; i++ {
			f := parent.GetInheritedField(i)
			if f.Name() == "" {
				if !c.dcFile.GetSortInheritanceByFile() {
					c.inheritedFields = append(c.inheritedFields, f)
				}
			} else {
				if !names[f.Name()] {
					names[f.Name()] = true
					c.inheritedFields = append(c.inheritedFields, f)
				}
			}
		}
	}

	for _, f := range c.fields {
		if f.Name() == "" {
			c.inheritedFields = append(c.inheritedFields, f)
		} else {
			if !names[f.Name()] {
				names[f.Name()] = true
			} else {
				c.shadowInheritedField(f.Name())
			}
			c.inheritedFields = append(c.inheritedFields, f)
		}
	}

	if c.dcFile.GetSortInheritanceByFile() {
		sort.SliceStable(c.inheritedFields, func(i, j int) bool {
			return c.inheritedFields[i].GetNumber() < c.inheritedFields[j].GetNumber()
		})
	}
}

func (c *dcClass) shadowInheritedField(name string) {
	for i, f := range c.inheritedFields {
		if f.Name() == name {
			c.inheritedFields = append(c.inheritedFields[:i], c.inheritedFields[i+1:]...)
			return
		}
	}
}

func (c *dcClass) AddField(f field) bool {
	f.SetClass(c)
	if c.dcFile != nil {
		c.dcFile.markInheritedFieldsStale()
	}

	if f.Name() != "" {
		if f.Name() == c.name {
			if c.constructor != nil {
				return false
			}
			if _, ok := f.(*atomicField); !ok {
				return false
			}
			c.constructor = f
			c.fieldsByName[f.Name()] = f
			return true
		}

		if _, exists := c.fieldsByName[f.Name()]; exists {
			return false
		}
		c.fieldsByName[f.Name()] = f
	}

	if c.dcFile != nil &&
		((c.dcFile.GetVirtualInheritance() && c.dcFile.GetSortInheritanceByFile()) || !c.IsStruct()) {
		if c.dcFile.GetMultipleInheritance() {
			c.dcFile.setNewIndexNumber(f)
		} else {
			f.SetNumber(c.GetNumInheritedFields())
		}
		c.fieldsByIndex[f.GetNumber()] = f
	}

	c.fields = append(c.fields, f)
	return true
}

func (c *dcClass) AddParent(parent *dcClass) {
	c.parents = append(c.parents, parent)
	c.dcFile.markInheritedFieldsStale()
}
