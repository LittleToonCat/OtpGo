package database

import (
	"errors"
	"fmt"
	"otpgo/core"
	. "otpgo/util"

	"otpgo/dc"

	"os"

	"gopkg.in/yaml.v2"
)

type YAMLInfo struct {
	Next Doid_t
}

type YAMLObject struct {
	ID    Doid_t
	Class string
	// We're using the MapSlice to preserve
	// insertion order.
	Fields yaml.MapSlice
}

type YAMLBackend struct {
	db        *DatabaseServer
	directory string
	next      Doid_t
}

func NewYAMLBackend(db *DatabaseServer, config Config) (bool, *YAMLBackend, error) {
	backend := &YAMLBackend{
		db:        db,
		directory: config.Directory,
		next:      0,
	}

	// Make configured directory if it does not exist yet
	err := os.MkdirAll(backend.directory, os.ModePerm)
	if err != nil {
		return false, nil, err
	}

	var info YAMLInfo
	if _, err := os.Stat(backend.directory + "/info.yaml"); errors.Is(err, os.ErrNotExist) {
		// Create new info.yaml file.
		info = YAMLInfo{
			Next: db.min,
		}

		res, err := yaml.Marshal(&info)
		if err != nil {
			return false, nil, err
		}

		f, err := os.Create(backend.directory + "/info.yaml")
		if err != nil {
			return false, nil, err
		}
		defer f.Close()

		_, err = f.Write(res)
		if err != nil {
			return false, nil, err
		}
	} else {
		// Open existing info.yaml file
		f, err := os.Open(backend.directory + "/info.yaml")
		if err != nil {
			return false, nil, err
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			return false, nil, err
		}

		data := make([]byte, fi.Size())
		_, err = f.Read(data)
		if err != nil {
			return false, nil, err
		}

		err = yaml.Unmarshal(data, &info)
		if err != nil {
			return false, nil, err
		}

		if info.Next == INVALID_DOID {
			return false, nil, errors.New("next is missing from info.yaml")
		}
	}

	backend.next = info.Next
	return true, backend, nil
}

func (b *YAMLBackend) AssignDoId() (Doid_t, error) {
	doId := b.next
	b.next++

	info := YAMLInfo{
		Next: b.next,
	}

	res, err := yaml.Marshal(&info)
	if err != nil {
		return 0, err
	}

	f, err := os.Create(b.directory + "/info.yaml")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	_, err = f.Write(res)
	if err != nil {
		return 0, err
	}

	return doId, nil
}

func (b *YAMLBackend) CreateStoredObject(dclass dc.DCClass, datas map[dc.DCField]dc.Vector_uchar,
	ctx uint32, sender Channel_t) {

	obj := &YAMLObject{
		ID:     0,
		Class:  dclass.Get_name(),
		Fields: yaml.MapSlice{},
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	defaults := map[dc.DCField]dc.Vector_uchar{}

	for i := 0; i < dclass.Get_num_inherited_fields(); i++ {
		field := dclass.Get_inherited_field(i)
		if field.Is_db() {
			if molecular, ok := field.As_molecular_field().(dc.DCMolecularField); ok {
				if molecular != dc.SwigcptrDCMolecularField(0) {
					continue
				}
			}

			data, ok := datas[field]
			if !ok {
				// Use default value instead if there is any.
				if field.Has_default_value() {
					// HACK: Because Get_default_value returns a pointer which will
					// become lost when accidentally deleted, we'd have to copy it.
					// into a new blob instance.
					value := field.Get_default_value()
					data = dc.NewVector_uchar()
					for i := int64(0); i < value.Size(); i++ {
						data.Add(value.Get(int(i)))
					}
					defaults[field] = data
				} else {
					// Move on.
					continue
				}
			}

			// Format the data into a string and store it:
			formattedString := field.Format_data(data, false)
			if formattedString == "" {
				b.db.log.Errorf("Failed to unpack field \"%s\"!\n%s", field.Get_name(), DumpVector(data))
				// Reply with an error code.
				dg := NewDatagram()
				dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
				dg.AddUint32(ctx)
				dg.AddUint8(1)
				dg.AddDoid(INVALID_DOID)
				b.db.RouteDatagram(dg)
				for _, data := range datas {
					dc.DeleteVector_uchar(data)
				}
				for _, data := range defaults {
					dc.DeleteVector_uchar(data)
				}
				return
			}
			obj.Fields = append(obj.Fields, yaml.MapItem{field.Get_name(), formattedString})
		}
	}

	doId, err := b.AssignDoId()
	if err != nil {
		b.db.log.Errorf("Failed to assign doId: %s", err.Error())
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(ctx)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		b.db.RouteDatagram(dg)
		for _, data := range datas {
			dc.DeleteVector_uchar(data)
		}
		for _, data := range defaults {
			dc.DeleteVector_uchar(data)
		}
		return
	}
	obj.ID = doId

	res, err := yaml.Marshal(&obj)
	if err != nil {
		b.db.log.Errorf("Error when Marshalling YAMLObject: %s", err.Error())
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(ctx)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		b.db.RouteDatagram(dg)
		for _, data := range datas {
			dc.DeleteVector_uchar(data)
		}
		for _, data := range defaults {
			dc.DeleteVector_uchar(data)
		}
		return
	}

	if _, err := os.Stat(fmt.Sprintf(b.directory+"/%d.yaml", doId)); err == nil {
		// File already exists.
		b.db.log.Errorf("%d.yaml already exists!", doId)
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(ctx)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		b.db.RouteDatagram(dg)
		for _, data := range datas {
			dc.DeleteVector_uchar(data)
		}
		for _, data := range defaults {
			dc.DeleteVector_uchar(data)
		}
		return
	}

	f, err := os.Create(fmt.Sprintf(b.directory+"/%d.yaml", doId))
	if err != nil {
		b.db.log.Errorf("Error when creating %d.yaml: %s", doId, err.Error())
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(ctx)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		b.db.RouteDatagram(dg)
		for _, data := range datas {
			dc.DeleteVector_uchar(data)
		}
		for _, data := range defaults {
			dc.DeleteVector_uchar(data)
		}
		return
	}

	defer f.Close()

	_, err = f.Write(res)
	if err != nil {
		b.db.log.Errorf("Error when writing to %d.yaml: %s", doId, err.Error())
		// Reply with an error code.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
		dg.AddUint32(ctx)
		dg.AddUint8(1)
		dg.AddDoid(INVALID_DOID)
		b.db.RouteDatagram(dg)
		for _, data := range datas {
			dc.DeleteVector_uchar(data)
		}
		for _, data := range defaults {
			dc.DeleteVector_uchar(data)
		}
		return
	}

	b.db.log.Debugf("Successfully created new %s object with ID: %v", obj.Class, obj.ID)

	// Send a successful response to the sender.
	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
	dg.AddUint32(ctx)
	dg.AddUint8(0) // return code
	dg.AddDoid(doId)
	b.db.RouteDatagram(dg)

	// Cleanup
	for _, data := range datas {
		dc.DeleteVector_uchar(data)
	}
	for _, data := range defaults {
		dc.DeleteVector_uchar(data)
	}
}

func (b *YAMLBackend) SendGetStoredValuesError(doId Doid_t, fields []string, ctx uint32, sender Channel_t) {
	// Reply with an error.
	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(ctx)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fields)))
	for _, field := range fields {
		dg.AddString(field)
	}
	dg.AddUint8(1) // Error code
	b.db.RouteDatagram(dg)
}

func (b *YAMLBackend) GetStoredValues(doId Doid_t, fields []string, ctx uint32, sender Channel_t) {
	if _, err := os.Stat(fmt.Sprintf(b.directory+"/%d.yaml", doId)); errors.Is(err, os.ErrNotExist) {
		b.db.log.Errorf("GetStoredValues: File %d.yaml does not exist!")
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	f, err := os.Open(fmt.Sprintf(b.directory+"/%d.yaml", doId))
	if err != nil {
		b.db.log.Error(err.Error())
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		b.db.log.Error(err.Error())
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	data := make([]byte, fi.Size())
	_, err = f.Read(data)
	if err != nil {
		b.db.log.Error(err.Error())
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	var obj YAMLObject
	err = yaml.Unmarshal(data, &obj)
	if err != nil {
		b.db.log.Error(err.Error())
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	if obj.ID != doId {
		b.db.log.Errorf(fmt.Sprintf("%d.yaml contains data for id %d instead!", doId, obj.ID))
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	// Unmarshal the fields map:
	objFields := make(map[string]string)
	for _, data := range obj.Fields {
		objFields[data.Key.(string)] = data.Value.(string)
	}

	dclass := core.DC.Get_class_by_name(obj.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", obj.Class, doId)
		b.SendGetStoredValuesError(doId, fields, ctx, sender)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedData := map[string]dc.Vector_uchar{}
	for _, field := range fields {
		dcField := dclass.Get_field_by_name(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, obj.Class)
			continue
		}

		if field == "DcObjectType" {
			// Return dclass type
			packedData[field] = dcField.Parse_string("\"" + obj.Class + "\"")
			continue
		}

		value, ok := objFields[field]
		if !ok {
			// Field not found, that's alright, continue on.
			continue
		}

		parsedData := dcField.Parse_string(value)
		if parsedData.Size() == 0 {
			b.db.log.Errorf("Failed to parse data for field \"%s\": %s", field, value)
			dc.DeleteVector_uchar(parsedData)
			continue
		}

		packedData[field] = parsedData
	}

	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_GET_STORED_VALUES_RESP)
	dg.AddUint32(ctx)
	dg.AddDoid(doId)
	dg.AddUint16(uint16(len(fields)))
	for _, field := range fields {
		dg.AddString(field)
	}
	dg.AddUint8(0) // Return code
	for _, field := range fields {
		if packedValue, ok := packedData[field]; ok {
			dg.AddUint16(uint16(packedValue.Size()))
			dg.AddVector(packedValue)
			dg.AddBool(true) // Found
		} else {
			dg.AddString("")
			dg.AddBool(false) // Not found
		}
	}
	b.db.RouteDatagram(dg)

	// Cleanup
	for _, data := range packedData {
		dc.DeleteVector_uchar(data)
	}

}

func (b *YAMLBackend) SetStoredValues(doId Doid_t, packedValues map[string]dc.Vector_uchar) {
	if _, err := os.Stat(fmt.Sprintf(b.directory+"/%d.yaml", doId)); errors.Is(err, os.ErrNotExist) {
		b.db.log.Errorf("SetStoredValues: File %d.yaml does not exist!")
		return
	}

	f, err := os.Open(fmt.Sprintf(b.directory+"/%d.yaml", doId))
	if err != nil {
		b.db.log.Error(err.Error())
		return
	}

	fi, err := f.Stat()
	if err != nil {
		b.db.log.Error(err.Error())
		return
	}

	data := make([]byte, fi.Size())
	_, err = f.Read(data)
	if err != nil {
		b.db.log.Error(err.Error())
		return
	}

	var obj YAMLObject
	err = yaml.Unmarshal(data, &obj)
	if err != nil {
		b.db.log.Error(err.Error())
		return
	}

	if obj.ID != doId {
		b.db.log.Errorf(fmt.Sprintf("%d.yaml contains data for id %d instead!", doId, obj.ID))
		return
	}

	f.Close()

	// Unmarshal the fields map:
	objFields := make(map[string]string)
	for _, data := range obj.Fields {
		objFields[data.Key.(string)] = data.Value.(string)
	}

	dclass := core.DC.Get_class_by_name(obj.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", obj.Class, doId)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	for field, value := range packedValues {
		dcField := dclass.Get_field_by_name(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, obj.Class)
			continue
		}

		formattedString := dcField.Format_data(value, false)
		if formattedString == "" {
			b.db.log.Errorf("Failed to unpack field \"%s\"!\n%s", field, DumpVector(value))
			continue
		}
		objFields[field] = formattedString
	}

	// Recreate MapSlice to preserve order:
	obj.Fields = yaml.MapSlice{}
	for i := 0; i < dclass.Get_num_inherited_fields(); i++ {
		field := dclass.Get_inherited_field(i)
		if field.Is_db() {
			if molecular, ok := field.As_molecular_field().(dc.DCMolecularField); ok {
				if molecular != dc.SwigcptrDCMolecularField(0) {
					continue
				}
			}

			if value, ok := objFields[field.Get_name()]; ok {
				obj.Fields = append(obj.Fields, yaml.MapItem{field.Get_name(), value})
			}
		}
	}

	res, err := yaml.Marshal(&obj)
	if err != nil {
		b.db.log.Errorf("Error when Marshalling YAMLObject: %s", err.Error())
		return
	}

	f, err = os.Create(fmt.Sprintf(b.directory+"/%d.yaml", doId))
	if err != nil {
		b.db.log.Errorf("Error when creating %d.yaml: %s", doId, err.Error())
		return
	}

	defer f.Close()

	_, err = f.Write(res)
	if err != nil {
		b.db.log.Errorf("Error when writing to %d.yaml: %s", doId, err.Error())
		return
	}
}
