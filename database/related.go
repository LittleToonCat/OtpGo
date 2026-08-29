//go:build !no_dbserver

package database

import (
	"reflect"

	"otpgo/core"
	"otpgo/dc"
	. "otpgo/util"

	"github.com/apex/log"
)

type GetRelatedRequest struct {
	Context       uint32
	ParentDoId    Doid_t
	ParentFields  []string
	RelationField string
	TargetClass   string
	TargetFields  []string
}

type relatedChildPacked struct {
	doId   Doid_t
	values map[string][]byte
}

func resolveRelationField(reg *ReferenceRegistry, parentClass, relationField, targetClass string) (name string, field dc.DCField, isList bool, ok bool) {
	if relationField == "" {
		var match Reference
		n := 0
		for _, ref := range reg.For(parentClass) {
			if ref.Target.GetName() == targetClass {
				match = ref
				n++
			}
		}
		if n != 1 {
			return "", nil, false, false
		}
		return match.FieldName, match.Field, match.IsList, true
	}

	for _, ref := range reg.For(parentClass) {
		if ref.FieldName == relationField {
			return ref.FieldName, ref.Field, ref.IsList, true
		}
	}
	cls := core.DC.GetClassByName(parentClass)
	if cls == nil {
		return "", nil, false, false
	}
	f := cls.GetFieldByName(relationField)
	if f == nil {
		return "", nil, false, false
	}
	list, _, err := referenceKind(f)
	if err != nil {
		return "", nil, false, false
	}
	return relationField, f, list, true
}

func docChildDOIDs(value interface{}) []Doid_t {
	var out []Doid_t
	var walk func(v interface{})
	walk = func(v interface{}) {
		if rv := reflect.ValueOf(v); rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			for i := 0; i < rv.Len(); i++ {
				walk(rv.Index(i).Interface())
			}
			return
		}
		if id := toDoid(v); id != 0 {
			out = append(out, id)
		}
	}
	walk(value)
	return out
}

func toDoid(v interface{}) Doid_t {
	switch n := v.(type) {
	case float64:
		return Doid_t(n)
	case int:
		return Doid_t(n)
	case int32:
		return Doid_t(n)
	case int64:
		return Doid_t(n)
	case uint:
		return Doid_t(n)
	case uint32:
		return Doid_t(n)
	case uint64:
		return Doid_t(n)
	}
	return 0
}

func packDocFieldsJSON(logger *log.Entry, clsName string, fieldNames []string, doc map[string]interface{}) map[string][]byte {
	cls := core.DC.GetClassByName(clsName)
	if cls == nil {
		logger.Errorf("packDocFieldsJSON: class %s does not exist", clsName)
		return nil
	}

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	out := make(map[string][]byte, len(fieldNames))
	for _, name := range fieldNames {
		dcField := cls.GetFieldByName(name)
		if dcField == nil {
			logger.Errorf("packDocFieldsJSON: field %s does not exist for class %s", name, clsName)
			continue
		}

		if name == "DcObjectType" {
			out[name] = dcField.ParseString("\"" + clsName + "\"")
			continue
		}

		value, ok := doc[name]
		if !ok {
			continue
		}

		packer.BeginPack(dcField)
		PackValue(packer, value, *logger)
		if !packer.EndPack() {
			logger.Errorf("packDocFieldsJSON: pack failed for %s.%s", clsName, name)
			packer.ClearData()
			continue
		}
		out[name] = packer.GetBytes()
		packer.ClearData()
	}
	return out
}

func (d *DatabaseServer) relatedChildDOIDs(req GetRelatedRequest, parentClass string, parentDoc map[string]interface{}) (ids []Doid_t, code uint8) {
	name, _, _, ok := resolveRelationField(d.references, parentClass, req.RelationField, req.TargetClass)
	if !ok {
		d.log.Warnf("GET_RELATED(%d): no relationship from %s to %s (field %q)",
			req.ParentDoId, parentClass, req.TargetClass, req.RelationField)
		return nil, 2
	}
	return docChildDOIDs(parentDoc[name]), 0
}

func (d *DatabaseServer) sendGetRelatedResp(sender Channel_t, ctx uint32, code uint8,
	parentFields []string, parentValues map[string][]byte,
	childFields []string, children []relatedChildPacked) {

	dg := NewDatagram()
	dg.AddServerHeader(sender, d.control, DBSERVER_GET_RELATED_RESP)
	dg.AddUint32(ctx)
	dg.AddUint8(code)

	if code != 0 {
		dg.AddUint16(0)
		dg.AddUint16(0)
		dg.AddUint16(0)
		d.RouteDatagram(dg)
		return
	}

	dg.AddUint16(uint16(len(parentFields)))
	for _, f := range parentFields {
		dg.AddString(f)
	}
	for _, f := range parentFields {
		v, found := parentValues[f]
		writeFieldValue(&dg, v, found)
	}

	dg.AddUint16(uint16(len(childFields)))
	for _, f := range childFields {
		dg.AddString(f)
	}
	dg.AddUint16(uint16(len(children)))
	for _, c := range children {
		dg.AddUint32(uint32(c.doId))
		for _, f := range childFields {
			v, found := c.values[f]
			writeFieldValue(&dg, v, found)
		}
	}

	d.RouteDatagram(dg)
}

func writeFieldValue(dg *Datagram, v []byte, found bool) {
	dg.AddUint16(uint16(len(v)))
	if len(v) > 0 {
		dg.AddData(v)
	}
	dg.AddBool(found)
}
