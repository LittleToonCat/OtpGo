package database

import (
	"context"
	"otpgo/core"
	. "otpgo/util"
	"time"

	dc "github.com/LittleToonCat/dcparser-go"
	"github.com/apex/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Globals struct {
	ID   string `bson:"_id"`
	DoId *GlobalsDoId `bson:"doid"`
}

type GlobalsDoId struct {
	Monotonic Doid_t `bson:"monotonic"`
	Free      []Doid_t `bson:"free"`
}

type StoredObject struct {
	ID     Doid_t `bson:"_id"`
	Class  string `bson:"dclass"`
	Fields primitive.D `bson:"fields"`
}

type MongoBackend struct {
	db *DatabaseServer
	client *mongo.Client
	globals *mongo.Collection
	objects *mongo.Collection
}

func UnpackDataToBsonArray(unpacker dc.DCPacker, array *bson.A, log log.Entry) {
	switch unpacker.Get_pack_type() {
	case dc.PT_invalid:
		log.Errorf("UnpackDataToBsonArray: PT_invalid reached!\n%s", DumpUnpacker(unpacker))
		*array = append(*array, "invalid")
	case dc.PT_double:
		*array = append(*array, unpacker.Unpack_double().(float64))
	case dc.PT_int:
		*array = append(*array, unpacker.Unpack_int().(int))
	case dc.PT_uint:
		*array = append(*array, unpacker.Unpack_uint().(uint))
	case dc.PT_int64:
		*array = append(*array, unpacker.Unpack_int64().(int64))
	case dc.PT_uint64:
		*array = append(*array, unpacker.Unpack_uint64().(uint64))
	case dc.PT_string:
		*array = append(*array, unpacker.Unpack_string().(string))
	case dc.PT_blob:
		vector := unpacker.Unpack_blob().(dc.Vector_uchar)
		data := []byte{}
		for i := int64(0); i < vector.Size(); i++ {
			data = append(data, vector.Get(int(i)))
		}
		*array = append(*array, data)
	default:
		// More nested fields, nest call this exact function.
		nestedArray := bson.A{}
		unpacker.Push()
		for unpacker.More_nested_fields() {
			UnpackDataToBsonArray(unpacker, &nestedArray, log)
		}
		unpacker.Pop()
		*array = append(*array, nestedArray)
	}

	log.Debugf("Resulting Array: %v", *array)
}

func UnpackDataToBsonDocument(unpacker dc.DCPacker, name string, doc *bson.D, log log.Entry) {
	switch unpacker.Get_pack_type() {
	case dc.PT_invalid:
		log.Errorf("UnpackDataToBsonDocument: PT_invalid reached!\n%s", DumpUnpacker(unpacker))
	case dc.PT_double:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_double().(float64)})
	case dc.PT_int:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_int().(int)})
	case dc.PT_uint:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_uint().(uint)})
	case dc.PT_int64:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_int64().(int64)})
	case dc.PT_uint64:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_uint64().(uint64)})
	case dc.PT_string:
		*doc = append(*doc, bson.E{name, unpacker.Unpack_string().(string)})
	case dc.PT_blob:
		vector := unpacker.Unpack_blob().(dc.Vector_uchar)
		data := []byte{}
		for i := int64(0); i < vector.Size(); i++ {
			data = append(data, vector.Get(int(i)))
		}
		*doc = append(*doc, bson.E{name, data})
	default:
		// If we reached here, that means it is a list
		// of nested fields (e.g. an array type, an atomic field, a
		// class parameter, or a switch case).
		//
		// We'll have to create an BSON array for these types.
		array := bson.A{}
		unpacker.Push()
		for unpacker.More_nested_fields() {
			UnpackDataToBsonArray(unpacker, &array, log)
		}
		unpacker.Pop()
		*doc = append(*doc, bson.E{name, array})
	}

	log.Debugf("Resulting Document: %v", *doc)
}

func PackBsonValue(packer dc.DCPacker, value interface{}) {
	switch packer.Get_pack_type() {
	case dc.PT_invalid:
		// TODO: Error out
	case dc.PT_double:
		if double, ok := value.(float64); ok {
			packer.Pack_double(double)
		}
	case dc.PT_int:
		fallthrough
	case dc.PT_uint:
		fallthrough
	case dc.PT_int64:
		fallthrough
	case dc.PT_uint64:
		if intValue, ok := value.(int32); ok {
			packer.Pack_int(int(intValue))
		} else if int64Value, ok := value.(int64); ok {
			packer.Pack_int64(int64Value)
		}
	case dc.PT_string:
		if stringValue, ok := value.(string); ok {
			packer.Pack_string(stringValue)
		}
	case dc.PT_blob:
		if binData, ok := value.(primitive.Binary); ok {
			packer.Pack_string(string(binData.Data))
		}
	default:
		array := value.(bson.A)
		packer.Push()
		for _, v := range array {
			PackBsonValue(packer, v)
		}
		packer.Pop()
	}
}

func NewMongoBackend(db *DatabaseServer, config Config) (bool, *MongoBackend, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.Server))
	if err != nil {
		return false, nil, err
	}

	backend := &MongoBackend{
		db: db,
		client: client,
		globals: client.Database(config.Database).Collection("globals"),
		objects: client.Database(config.Database).Collection("objects"),
	}

	// Create globals collection if it doesn't exist already.
	var result Globals
	err = backend.globals.FindOne(context.Background(), bson.D{{"_id", "GLOBALS"}}).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			global := Globals{
				ID: "GLOBALS",
				DoId: &GlobalsDoId{
					Monotonic: db.min,
					Free: make([]Doid_t, 0),
				},
			}

			result, error := backend.globals.InsertOne(context.Background(), global)
			if error != nil {
				return false, nil, error
			}
			db.log.Infof("Inserted new %v document,", result.InsertedID)
		} else {
			return false, nil, err
		}
	}

	return true, backend, nil
}

func (b * MongoBackend) AssignDoId() Doid_t {
	monotonicDoId := b.AssignDoIdMonotonic()
	if monotonicDoId != INVALID_DOID {
		return monotonicDoId
	}

	// TODO: AssignDoIdReuse
	return INVALID_DOID
}

func (b *MongoBackend) AssignDoIdMonotonic() Doid_t {
	filter := bson.D{{"_id", "GLOBALS"},
					{"doid.monotonic", bson.D{{"$gte", b.db.min}}},
					{"doid.monotonic", bson.D{{"$lte", b.db.max}}}}

	update := bson.M{"$inc": bson.M{"doid.monotonic": 1}}

	var globals Globals
	err := b.globals.FindOneAndUpdate(context.Background(), filter, update).Decode(&globals)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: %s", err.Error())
		return INVALID_DOID
	}
	return globals.DoId.Monotonic
}

func (b *MongoBackend) CreateStoredObject(dclass dc.DCClass, datas map[dc.DCField]dc.Vector_uchar,
										  ctx uint32, sender Channel_t) {

	var doc bson.D

	DCLock.Lock()
	defer DCLock.Unlock()

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
				// Use default value instead if there is any, or use the
				// field type's empty value if it's a required field.
				if field.Has_default_value() || field.Is_required() {
					// HACK: Because Get_default_value returns a pointer which will
					// become lost when accidentally deleted, we'd have to copy it.
					// into a new blob instance.
					value := field.Get_default_value()
					data = dc.NewVector_uchar()
					for i := int64(0); i < value.Size(); i++ {
						data.Add(value.Get(int(i)))
					}
				} else {
					// Move on.
					continue
				}
			}

			unpacker := dc.NewDCPacker()
			defer dc.DeleteDCPacker(unpacker)

			unpacker.Set_unpack_data(data)
			unpacker.Begin_unpack(field)

			UnpackDataToBsonDocument(unpacker, field.Get_name(), &doc, *b.db.log)

			if !unpacker.End_unpack() {
				b.db.log.Errorf("Failed to unpack field \"%s\"!\n%s", field.Get_name(), DumpUnpacker(unpacker))
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
			}
		}
	}

	doId := b.AssignDoId()
	if doId == INVALID_DOID {
		b.db.log.Error("Unable to assign a doId!")
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
		return
	}

	obj := StoredObject{
		ID: doId,
		Class: dclass.Get_name(),
		Fields: doc,
	}
	res, err := b.objects.InsertOne(context.Background(), obj)
	if err != nil {
		b.db.log.Errorf("Insertion of %s object failed: %s", obj.Class, err.Error())
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
		return
	}

	b.db.log.Debugf("Successfully created new %s object with ID: %v", obj.Class, res.InsertedID)

	// Send a successful response to the sender.
	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
	dg.AddUint32(ctx)
	dg.AddUint8(0) // return code
	dg.AddDoid(doId)
	b.db.RouteDatagram(dg)
}

func (b *MongoBackend) GetStoredValues(doId Doid_t, fields []string, ctx uint32, sender Channel_t) {
	filter := bson.M{"_id": doId}

	var object StoredObject
	err := b.objects.FindOne(context.Background(), filter).Decode(&object)
	if err != nil {
		b.db.log.Errorf("Failed to retrieve object %d from database: %s", doId, err.Error())

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
		return
	}

	dclass := core.DC.Get_class_by_name(object.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", object.Class, doId)

		// Reply with an error.
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_GET_STORED_VALUES_RESP)
		dg.AddUint32(ctx)
		dg.AddDoid(doId)
		dg.AddUint16(uint16(len(fields)))
		for _, field := range fields {
			dg.AddString(field)
		}
		dg.AddUint8(2) // Error code
		b.db.RouteDatagram(dg)
		return
	}


	// Marshal the document and unmarshal it back to a Map.
	doc, _ := bson.Marshal(object.Fields)
	fieldsMap := make(bson.M)
	_ = bson.Unmarshal(doc, &fieldsMap)

	DCLock.Lock()
	defer DCLock.Unlock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedData := map[string]dc.Vector_uchar{}
	for _, field := range fields {
		dcField := dclass.Get_field_by_name(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, object.Class)
			continue
		}

		if field == "DcObjectType" {
			// Return dclass type
			packedData[field] = dcField.Parse_string(object.Class)
			continue
		}

		value, ok := fieldsMap[field]
		if !ok {
			// Field not found, that's alright, continue on.
			continue
		}
		packer.Begin_pack(dcField)
		PackBsonValue(packer, value)
		if !packer.End_pack() {
			b.db.log.Errorf("Error has occurred when packing field \"%s\"", field)
			packer.Clear_data()
			continue
		}
		packedData[field] = packer.Get_bytes()
		packer.Clear_data()
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

func (b *MongoBackend) SetStoredValues(doId Doid_t, packedValues map[string]dc.Vector_uchar) {
	filter := bson.M{"_id": doId}

	var object StoredObject
	err := b.objects.FindOne(context.Background(), filter).Decode(&object)
	if err != nil {
		b.db.log.Errorf("Failed to retrieve object %d from database: %s", doId, err.Error())
		return
	}

	dclass := core.DC.Get_class_by_name(object.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", object.Class, doId)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	var setDoc bson.D
	var unsetDoc bson.D
	for field, value := range packedValues {
		if value.Size() == 0 {
			unsetDoc = append(unsetDoc, bson.E{"fields." + field, ""})
			continue
		}
		dcField := dclass.Get_field_by_name(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, object.Class)
			continue
		}

		unpacker.Set_unpack_data(value)
		unpacker.Begin_unpack(dcField)
		b.db.log.Debugf("Beginning unpack field \"%s\"\n%s", field, DumpUnpacker(unpacker))
		UnpackDataToBsonDocument(unpacker, "fields." + field, &setDoc, *b.db.log)
		if !unpacker.End_unpack() {
			b.db.log.Errorf("Failed to unpack field \"%s\"!  Update aborted.\n%s", field, DumpUnpacker(unpacker))

			for _, data := range packedValues {
				dc.DeleteVector_uchar(data)
			}
			return
		}
	}

	for _, data := range packedValues {
		dc.DeleteVector_uchar(data)
	}

	if setDoc == nil && unsetDoc == nil {
		b.db.log.Warnf("Nothing to do for update to object %s(%d).", object.Class, doId)
		return
	}
	update := bson.M{}
	if setDoc != nil {
		update["$set"] = setDoc
	}
	if unsetDoc != nil {
		update["$unset"] = unsetDoc
	}
	result, err := b.objects.UpdateOne(context.Background(), filter, update)
	if err != nil {
		b.db.log.Errorf("An error has occured when updating %s(%d): %s", object.Class, doId, err.Error())
	}

	if result.MatchedCount == 1 && result.ModifiedCount == 1 {
		b.db.log.Debugf("Successfully updated object %s(%d)", object.Class, doId)
	}

}
