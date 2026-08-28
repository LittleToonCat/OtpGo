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

	"otpgo/core"
	"otpgo/dc"
	. "otpgo/util"
)

// Reference is one resolved relationship.
type Reference struct {
	ClassName string
	FieldName string
	Field     dc.DCField
	Target    *dc.DCClass
	IsList    bool
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
		field := cls.GetFieldByName(rel.Field)
		if field == nil {
			return nil, fmt.Errorf("relationship: field %q does not exist on class %q", rel.Field, rel.Class)
		}
		if !field.IsDb() {
			return nil, fmt.Errorf("relationship %s.%s: field is not db-backed", rel.Class, rel.Field)
		}

		isList, err := referenceKind(field)
		if err != nil {
			return nil, fmt.Errorf("relationship %s.%s: %w", rel.Class, rel.Field, err)
		}

		ref := Reference{
			ClassName: rel.Class,
			FieldName: rel.Field,
			Field:     field,
			Target:    target,
			IsList:    isList,
		}
		reg.byClass[rel.Class] = append(reg.byClass[rel.Class], ref)
		reg.all = append(reg.all, ref)
	}

	return reg, nil
}

// referenceKind reports whether a field is a scalar uint reference (false) or a
// list of uint references (true), and errors for anything else.
func referenceKind(field dc.DCField) (isList bool, err error) {
	switch field.PackType() {
	case dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64:
		return false, nil
	case dc.PTArray:
		switch field.GetNestedField(0).PackType() {
		case dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64:
			return true, nil
		}
		return false, fmt.Errorf("array element is not a uint (struct-list references are not supported)")
	}
	return false, fmt.Errorf("field is not a uint or uint[] reference")
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
	collectDOIDs(p, &out)

	if !p.EndUnpack() {
		return nil, fmt.Errorf("failed to unpack reference field %s.%s", ref.ClassName, ref.FieldName)
	}
	return out, nil
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
