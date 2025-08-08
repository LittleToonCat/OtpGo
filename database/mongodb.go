package database

import (
	"context"
	"fmt"
	"otpgo/core"
	. "otpgo/util"
	"time"

	"otpgo/dc"

	"github.com/apex/log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Globals struct {
	ID   string       `bson:"_id"`
	DoId *GlobalsDoId `bson:"doid"`
}

type GlobalsDoId struct {
	Monotonic Doid_t   `bson:"monotonic"`
	Free      []Doid_t `bson:"free"`
}

type StoredObject struct {
	ID     Doid_t      `bson:"_id"`
	Class  string      `bson:"dclass"`
	Fields primitive.D `bson:"fields"`
}

type MongoBackend struct {
	db      *DatabaseServer
	client  *mongo.Client
	globals *mongo.Collection
	objects *mongo.Collection
}

func UnpackDataToBsonArray(unpacker dc.DCPacker, array *bson.A, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToBsonArray: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
		*array = append(*array, "invalid")
	case dc.PTDouble:
		*array = append(*array, unpacker.UnpackDouble().(float64))
	case dc.PTInt:
		*array = append(*array, unpacker.UnpackInt().(int))
	case dc.PTUint:
		*array = append(*array, unpacker.UnpackUint().(uint))
	case dc.PTInt64:
		*array = append(*array, unpacker.UnpackInt64().(int64))
	case dc.PTUint64:
		*array = append(*array, unpacker.UnpackUint64().(uint64))
	case dc.PTString:
		*array = append(*array, unpacker.UnpackString().(string))
	case dc.PTBlob:
		vector := unpacker.UnpackBlob().(dc.Vector)
		data := []byte{}
		for i := int64(0); i < vector.Size(); i++ {
			data = append(data, vector.Get(int(i)))
		}
		*array = append(*array, data)
		dc.DeleteVector(vector)
	default:
		// More nested fields, nest call this exact function.
		nestedArray := bson.A{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToBsonArray(unpacker, &nestedArray, log)
		}
		unpacker.Pop()
		*array = append(*array, nestedArray)
	}

	log.Debugf("Resulting Array: %v", *array)
}

func UnpackDataToBsonDocument(unpacker dc.DCPacker, name string, doc *bson.D, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToBsonDocument: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
	case dc.PTDouble:
		*doc = append(*doc, bson.E{name, unpacker.UnpackDouble().(float64)})
	case dc.PTInt:
		*doc = append(*doc, bson.E{name, unpacker.UnpackInt().(int)})
	case dc.PTUint:
		*doc = append(*doc, bson.E{name, unpacker.UnpackUint().(uint)})
	case dc.PTInt64:
		*doc = append(*doc, bson.E{name, unpacker.UnpackInt64().(int64)})
	case dc.PTUint64:
		*doc = append(*doc, bson.E{name, unpacker.UnpackUint64().(uint64)})
	case dc.PTString:
		*doc = append(*doc, bson.E{name, unpacker.UnpackString().(string)})
	case dc.PTBlob:
		vector := unpacker.UnpackBlob().(dc.Vector)
		data := []byte{}
		for i := int64(0); i < vector.Size(); i++ {
			data = append(data, vector.Get(int(i)))
		}
		*doc = append(*doc, bson.E{name, data})
		dc.DeleteVector(vector)
	default:
		// If we reached here, that means it is a list
		// of nested fields (e.g. an array type, an atomic field, a
		// class parameter, or a switch case).
		//
		// We'll have to create an BSON array for these types.
		array := bson.A{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToBsonArray(unpacker, &array, log)
		}
		unpacker.Pop()
		*doc = append(*doc, bson.E{name, array})
	}

	log.Debugf("Resulting Document: %v", *doc)
}

func PackBsonValue(packer dc.DCPacker, value interface{}) {
	switch packer.GetPackType() {
	case dc.PTInvalid:
		// TODO: Error out
	case dc.PTDouble:
		if double, ok := value.(float64); ok {
			packer.PackDouble(double)
		}
	case dc.PTInt:
		fallthrough
	case dc.PTUint:
		fallthrough
	case dc.PTInt64:
		fallthrough
	case dc.PTUint64:
		if intValue, ok := value.(int32); ok {
			packer.PackInt(int(intValue))
		} else if int64Value, ok := value.(int64); ok {
			packer.PackInt64(int64Value)
		}
	case dc.PTString:
		if stringValue, ok := value.(string); ok {
			packer.PackString(stringValue)
		}
	case dc.PTBlob:
		if binData, ok := value.(primitive.Binary); ok {
			packer.PackString(string(binData.Data))
		}
	default:
		if array, ok := value.(bson.A); ok {
			packer.Push()
			for _, v := range array {
				PackBsonValue(packer, v)
			}
			packer.Pop()
		} else if doc, ok := value.(bson.M); ok {
			// This is for Astron database compatibility.
			// They store atomic (method) field values as a document,
			// using their names as keys.  They will be replaced as an
			// array upon restorage.
			packer.Push()
			numValues := len(doc)
			for i := 0; i < numValues; i++ {
				field := packer.GetCurrentField().AsField().(dc.DCField)
				name := field.GetName()
				if name == "" {
					name = fmt.Sprintf("_%d", i)
				}
				if v, ok := doc[name]; ok {
					PackBsonValue(packer, v)
				} else {
					// Maybe "_{element_number}" is the key instead.
					if name != "" {
						name = fmt.Sprintf("_%d", i)
						if v, ok := doc[name]; ok {
							PackBsonValue(packer, v)
						}
					}
				}
			}
			packer.Pop()
		}
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
		db:      db,
		client:  client,
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
					Free:      make([]Doid_t, 0),
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

func (b *MongoBackend) AssignDoId() Doid_t {
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

func (b *MongoBackend) CreateStoredObject(dclass dc.DCClass, datas map[dc.DCField]dc.Vector,
	ctx uint32, sender Channel_t) {

	var doc bson.D

	DCLock.Lock()
	defer DCLock.Unlock()

	defaults := map[dc.DCField]dc.Vector{}

	for i := 0; i < dclass.GetNumInheritedFields(); i++ {
		field := dclass.GetInheritedField(i)
		if field.IsDb() {
			if molecular, ok := field.AsMolecularField().(dc.DCMolecularField); ok {
				if molecular != dc.SwigcptrDCMolecularField(0) {
					continue
				}
			}

			data, ok := datas[field]
			if !ok {
				// Use default value instead if there is any.
				if field.HasDefaultValue() {
					// HACK: Because GetDefaultValue returns a pointer which will
					// become lost when accidentally deleted, we'd have to copy it.
					// into a new blob instance.
					value := field.GetDefaultValue()
					data = dc.NewVector()
					for i := int64(0); i < value.Size(); i++ {
						data.Add(value.Get(int(i)))
					}
					defaults[field] = data
				} else {
					// Move on.
					continue
				}
			}

			unpacker := dc.NewDCPacker()
			defer dc.DeleteDCPacker(unpacker)

			unpacker.SetUnpackData(data)
			unpacker.BeginUnpack(field)

			UnpackDataToBsonDocument(unpacker, field.GetName(), &doc, *b.db.log)

			if !unpacker.EndUnpack() {
				b.db.log.Errorf("Failed to unpack field \"%s\"!\n%s", field.GetName(), DumpUnpacker(unpacker))
				// Reply with an error code.
				dg := NewDatagram()
				dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
				dg.AddUint32(ctx)
				dg.AddUint8(1)
				dg.AddDoid(INVALID_DOID)
				b.db.RouteDatagram(dg)
				for _, data := range datas {
					dc.DeleteVector(data)
				}
				for _, data := range defaults {
					dc.DeleteVector(data)
				}
			}
		}
	}

	if doc == nil {
		// Nothing has been done to the document, create a empty one.
		doc = bson.D{}
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
			dc.DeleteVector(data)
		}
		for _, data := range defaults {
			dc.DeleteVector(data)
		}
		return
	}

	obj := StoredObject{
		ID:     doId,
		Class:  dclass.GetName(),
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
			dc.DeleteVector(data)
		}
		for _, data := range defaults {
			dc.DeleteVector(data)
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

	// Cleanup
	for _, data := range datas {
		dc.DeleteVector(data)
	}
	for _, data := range defaults {
		dc.DeleteVector(data)
	}
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

	dclass := core.DC.GetClassByName(object.Class)
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

	packedData := map[string]dc.Vector{}
	for _, field := range fields {
		dcField := dclass.GetFieldByName(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, object.Class)
			continue
		}

		if field == "DcObjectType" {
			// Return dclass type
			packedData[field] = dcField.ParseString("\"" + object.Class + "\"")
			continue
		}

		value, ok := fieldsMap[field]
		if !ok {
			// Field not found, that's alright, continue on.
			continue
		}
		packer.BeginPack(dcField)
		PackBsonValue(packer, value)
		if !packer.EndPack() {
			b.db.log.Errorf("Error has occurred when packing field \"%s\"", field)
			packer.ClearData()
			continue
		}
		packedData[field] = packer.GetBytes()
		packer.ClearData()
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
		dc.DeleteVector(data)
	}
}

func (b *MongoBackend) SetStoredValues(doId Doid_t, packedValues map[string]dc.Vector) {
	filter := bson.M{"_id": doId}

	var object StoredObject
	err := b.objects.FindOne(context.Background(), filter).Decode(&object)
	if err != nil {
		b.db.log.Errorf("Failed to retrieve object %d from database: %s", doId, err.Error())
		return
	}

	dclass := core.DC.GetClassByName(object.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", object.Class, doId)
		return
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	var writes []mongo.WriteModel
	for field, value := range packedValues {
		if value.Size() == 0 {
			updateModel := mongo.NewUpdateOneModel()
			updateModel.SetFilter(filter)
			updateModel.SetUpdate(bson.M{"$unset": bson.M{"fields." + field: ""}})
			writes = append(writes, updateModel)
			continue
		}

		dcField := dclass.GetFieldByName(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, object.Class)
			continue
		}

		unpacker.SetUnpackData(value)
		unpacker.BeginUnpack(dcField)
		b.db.log.Debugf("Beginning unpack field \"%s\"\n%s", field, DumpUnpacker(unpacker))
		var setDoc bson.D
		UnpackDataToBsonDocument(unpacker, "fields."+field, &setDoc, *b.db.log)
		if !unpacker.EndUnpack() {
			b.db.log.Errorf("Failed to unpack field \"%s\"! Update aborted.\n%s", field, DumpUnpacker(unpacker))

			for _, data := range packedValues {
				dc.DeleteVector(data)
			}
			return
		}

		updateModel := mongo.NewUpdateOneModel()
		updateModel.SetFilter(filter)
		updateModel.SetUpdate(bson.M{"$set": setDoc})
		writes = append(writes, updateModel)
	}

	for _, data := range packedValues {
		dc.DeleteVector(data)
	}

	if len(writes) == 0 {
		b.db.log.Warnf("Nothing to do for update to object %s(%d).", object.Class, doId)
		return
	}

	bulkOption := options.BulkWrite().SetOrdered(false)
	result, err := b.objects.BulkWrite(context.Background(), writes, bulkOption)
	if err != nil {
		b.db.log.Errorf("An error has occurred when updating %s(%d): %s", object.Class, doId, err.Error())
		return
	}

	if result.ModifiedCount > 0 {
		b.db.log.Debugf("Successfully updated object %s(%d)", object.Class, doId)
	}
}
