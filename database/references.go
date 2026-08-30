//go:build !no_dbserver

package database

// ReferenceRegistry holds the configured relationships between db-backed fields
// and the dclasses they reference (see core.RelationshipConfig / otp.yml
// "relationships"). It is consumed by the Postgres backend to build JSONB
// indexes and, when enabled, to enforce referential integrity on writes.
//
// The registry never inspects stored JSON - reference DOIDs are always taken
// from the packed DC bytes carried in a CREATE/SET datagram.

import (
	"fmt"
	"strconv"
	"strings"

	"otpgo/core"
	"otpgo/dc"
	. "otpgo/util"
)

// Reference is one resolved relationship.
type Reference struct {
	ClassName string
	FieldName string
	ElemIndex int
	Field     dc.DCField
	Target    *dc.DCClass
	IsList    bool
	Atomic    bool
}

func splitFieldIndex(spec string) (field string, index int, err error) {
	open := strings.IndexByte(spec, '[')
	if open < 0 {
		return spec, -1, nil
	}
	if !strings.HasSuffix(spec, "]") || open == 0 {
		return "", 0, fmt.Errorf("malformed field index in %q", spec)
	}
	idxStr := spec[open+1 : len(spec)-1]
	idx, convErr := strconv.Atoi(idxStr)
	if convErr != nil || idx < 0 {
		return "", 0, fmt.Errorf("invalid field index %q in %q", idxStr, spec)
	}
	return spec[:open], idx, nil
}

type ReferenceRegistry struct {
	byClass map[string][]Reference
	all     []Reference
}

// LoadReferenceRegistry resolves every relationship entry against the loaded DC
// file, returning a descriptive error if any entry is invalid.
func LoadReferenceRegistry(rels []core.RelationshipConfig, dcf *dc.DCFile) (*ReferenceRegistry, error) {
	reg := &ReferenceRegistry{byClass: make(map[string][]Reference)}

	for _, rel := range rels {
		cls := dcf.GetClassByName(rel.Class)
		if cls == nil {
			return nil, fmt.Errorf("relationship: class %q does not exist", rel.Class)
		}
		target := dcf.GetClassByName(rel.Target)
		if target == nil {
			return nil, fmt.Errorf("relationship %s.%s: target class %q does not exist", rel.Class, rel.Field, rel.Target)
		}
		fieldName, elemIndex, err := splitFieldIndex(rel.Field)
		if err != nil {
			return nil, fmt.Errorf("relationship %s.%s: %w", rel.Class, rel.Field, err)
		}
		field := cls.GetFieldByName(fieldName)
		if field == nil {
			return nil, fmt.Errorf("relationship: field %q does not exist on class %q", fieldName, rel.Class)
		}
		if !field.IsDb() {
			return nil, fmt.Errorf("relationship %s.%s: field is not db-backed", rel.Class, fieldName)
		}

		isList, atomic, err := referenceKind(field, elemIndex)
		if err != nil {
			return nil, fmt.Errorf("relationship %s.%s: %w", rel.Class, rel.Field, err)
		}

		ref := Reference{
			ClassName: rel.Class,
			FieldName: fieldName,
			ElemIndex: elemIndex,
			Field:     field,
			Target:    target,
			IsList:    isList,
			Atomic:    atomic,
		}
		reg.byClass[rel.Class] = append(reg.byClass[rel.Class], ref)
		reg.all = append(reg.all, ref)
	}

	return reg, nil
}

func referenceKind(field dc.DCField, elemIndex int) (isList bool, atomic bool, err error) {
	switch field.PackType() {
	case dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64:
		if elemIndex >= 0 {
			return false, false, fmt.Errorf("element index [%d] given but field is a scalar", elemIndex)
		}
		return false, false, nil
	case dc.PTArray:
		elem := field.GetNestedField(0)
		var subPack dc.DCPackType
		if elemIndex >= 0 && elemIndex < elem.NumNestedFields() {
			subPack = elem.GetNestedField(elemIndex).PackType()
		}
		if err := checkListElement(elem.PackType(), elem.NumNestedFields(), subPack, elemIndex); err != nil {
			return false, false, err
		}
		return true, false, nil
	case dc.PTField:
		if field.NumNestedFields() != 1 {
			return false, false, fmt.Errorf("atomic field wraps %d values; only single-value atomics can be references", field.NumNestedFields())
		}
		inner := field.GetNestedField(0)
		if inner.PackType() == dc.PTArray {
			elem := inner.GetNestedField(0)
			var subPack dc.DCPackType
			if elemIndex >= 0 && elemIndex < elem.NumNestedFields() {
				subPack = elem.GetNestedField(elemIndex).PackType()
			}
			if err := checkListElement(elem.PackType(), elem.NumNestedFields(), subPack, elemIndex); err != nil {
				return false, false, err
			}
			return true, true, nil
		}
		if elemIndex >= 0 {
			return false, false, fmt.Errorf("element index [%d] given but atomic field does not wrap an array", elemIndex)
		}
		if isUintPackType(inner.PackType()) {
			return false, true, nil
		}
		return false, false, fmt.Errorf("atomic field does not wrap a uint or uint[]")
	}
	return false, false, fmt.Errorf("field is not a uint or uint[] reference")
}

func checkListElement(elemPack dc.DCPackType, numSub int, subPack dc.DCPackType, elemIndex int) error {
	if elemIndex < 0 {
		if isUintPackType(elemPack) {
			return nil
		}
		return fmt.Errorf("array element is not a uint; specify an element index (e.g. field[0]) for struct-list references")
	}
	if elemPack != dc.PTClass && elemPack != dc.PTField {
		return fmt.Errorf("element index [%d] given but array element is not a struct", elemIndex)
	}
	if elemIndex >= numSub {
		return fmt.Errorf("element index [%d] out of range; struct element has %d sub-field(s)", elemIndex, numSub)
	}
	if !isUintPackType(subPack) {
		return fmt.Errorf("struct sub-field %d is not a uint", elemIndex)
	}
	return nil
}

func isUintPackType(pt dc.DCPackType) bool {
	switch pt {
	case dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64:
		return true
	}
	return false
}

func (r *ReferenceRegistry) Empty() bool { return r == nil || len(r.all) == 0 }

func (r *ReferenceRegistry) For(className string) []Reference {
	if r == nil {
		return nil
	}
	return r.byClass[className]
}

func (r *ReferenceRegistry) All() []Reference {
	if r == nil {
		return nil
	}
	return r.all
}

// ExtractDOIDs returns the non-zero DOIDs contained in a packed reference field
// value. `packed` is the raw DC field blob from a CREATE/SET datagram.
func (r *ReferenceRegistry) ExtractDOIDs(ref Reference, packed []byte) ([]Doid_t, error) {
	if len(packed) == 0 {
		return nil, nil
	}

	p := dc.NewDCPacker()
	defer dc.DeleteDCPacker(p)

	p.SetUnpackData(packed)
	p.BeginUnpack(ref.Field)

	var out []Doid_t
	if ref.ElemIndex >= 0 {
		collectIndexedDOIDs(p, ref.ElemIndex, &out)
	} else {
		collectDOIDs(p, &out)
	}

	if !p.EndUnpack() {
		return nil, fmt.Errorf("failed to unpack reference field %s.%s", ref.ClassName, ref.FieldName)
	}
	return out, nil
}

func collectIndexedDOIDs(p *dc.DCPacker, idx int, out *[]Doid_t) {
	if p.GetPackType() == dc.PTField {
		p.Push()
		for p.MoreNestedFields() {
			collectIndexedDOIDs(p, idx, out)
		}
		p.Pop()
		return
	}

	p.Push()
	for p.MoreNestedFields() {
		p.Push()
		for i := 0; p.MoreNestedFields(); i++ {
			if i == idx {
				if v := p.UnpackUint(); v != 0 {
					*out = append(*out, Doid_t(v))
				}
			} else {
				p.UnpackSkip()
			}
		}
		p.Pop()
	}
	p.Pop()
}

func collectDOIDs(p *dc.DCPacker, out *[]Doid_t) {
	switch p.GetPackType() {
	case dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64:
		if v := p.UnpackUint(); v != 0 {
			*out = append(*out, Doid_t(v))
		}
	case dc.PTInvalid, dc.PTDouble, dc.PTString, dc.PTBlob:
		p.UnpackSkip()
	default:
		p.Push()
		for p.MoreNestedFields() {
			collectDOIDs(p, out)
		}
		p.Pop()
	}
}
