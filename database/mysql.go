package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"otpgo/core"
	. "otpgo/util"
	"time"

	"otpgo/dc"

	"github.com/apex/log"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLGlobals struct {
	ID   string            `json:"_id"`
	DoId *MySQLGlobalsDoId `json:"doid"`
}

type MySQLGlobalsDoId struct {
	Monotonic Doid_t   `json:"monotonic"`
	Free      []Doid_t `json:"free"`
}

type MySQLStoredObject struct {
	ID     int64                  `json:"_id"`
	Class  string                 `json:"dclass"`
	Fields map[string]interface{} `json:"fields"`
}

type MySQLBackend struct {
	db     *DatabaseServer
	dbConn *sql.DB
}

func UnpackDataToArray(unpacker dc.DCPacker, array *[]interface{}, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToArray: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
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
		nestedArray := []interface{}{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToArray(unpacker, &nestedArray, log)
		}
		unpacker.Pop()
		*array = append(*array, nestedArray)
	}

	log.Debugf("Resulting Array: %v", *array)
}

func UnpackDataToDocument(unpacker dc.DCPacker, name string, doc map[string]interface{}, log log.Entry) {
	switch unpacker.GetPackType() {
	case dc.PTInvalid:
		log.Errorf("UnpackDataToDocument: PTInvalid reached!\n%s", DumpUnpacker(unpacker))
	case dc.PTDouble:
		doc[name] = unpacker.UnpackDouble().(float64)
	case dc.PTInt:
		doc[name] = unpacker.UnpackInt().(int)
	case dc.PTUint:
		doc[name] = unpacker.UnpackUint().(uint)
	case dc.PTInt64:
		doc[name] = unpacker.UnpackInt64().(int64)
	case dc.PTUint64:
		doc[name] = unpacker.UnpackUint64().(uint64)
	case dc.PTString:
		doc[name] = unpacker.UnpackString().(string)
	case dc.PTBlob:
		vector := unpacker.UnpackBlob().(dc.Vector)
		data := []byte{}
		for i := int64(0); i < vector.Size(); i++ {
			data = append(data, vector.Get(int(i)))
		}
		doc[name] = data
		dc.DeleteVector(vector)
	default:
		// If we reached here, that means it is a list
		// of nested fields (e.g. an array type, an atomic field, a
		// class parameter, or a switch case).
		//
		// We'll have to create an array for these types.
		array := []interface{}{}
		unpacker.Push()
		for unpacker.MoreNestedFields() {
			UnpackDataToArray(unpacker, &array, log)
		}
		unpacker.Pop()
		doc[name] = array
	}

	log.Debugf("Resulting Document: %v", doc)
}

func PackValue(packer dc.DCPacker, value interface{}, log log.Entry) {
	if array, ok := value.([]interface{}); ok && len(array) == 1 {
		switch packer.GetPackType() {
		case dc.PTDouble, dc.PTInt, dc.PTUint, dc.PTInt64, dc.PTUint64, dc.PTString, dc.PTBlob:
			value = array[0]
		}
	}

	switch packer.GetPackType() {
	case dc.PTInvalid:
		// TODO: Error out
	case dc.PTDouble:
		if double, ok := value.(float64); ok {
			packer.PackDouble(double)
		} else if intValue, ok := value.(int64); ok {
			packer.PackDouble(float64(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackDouble(float64(intValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if floatVal, err := jsonNumber.Float64(); err == nil {
				packer.PackDouble(floatVal)
			}
		}
	case dc.PTInt:
		if intValue, ok := value.(int64); ok {
			packer.PackInt(int(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackInt(intValue)
		} else if floatValue, ok := value.(float64); ok {
			packer.PackInt(int(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackInt(int(intVal))
			}
		}
	case dc.PTUint:
		if intValue, ok := value.(int64); ok {
			packer.PackUint(uint(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackUint(uint(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackUint(uint(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackUint(uint(intVal))
			}
		}
	case dc.PTInt64:
		if intValue, ok := value.(int64); ok {
			packer.PackInt64(intValue)
		} else if intValue, ok := value.(int); ok {
			packer.PackInt64(int64(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackInt64(int64(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackInt64(intVal)
			}
		}
	case dc.PTUint64:
		if intValue, ok := value.(int64); ok {
			packer.PackUint64(uint64(intValue))
		} else if intValue, ok := value.(int); ok {
			packer.PackUint64(uint64(intValue))
		} else if floatValue, ok := value.(float64); ok {
			packer.PackUint64(uint64(floatValue))
		} else if jsonNumber, ok := value.(json.Number); ok {
			if intVal, err := jsonNumber.Int64(); err == nil {
				packer.PackUint64(uint64(intVal))
			}
		}
	case dc.PTString:
		if stringValue, ok := value.(string); ok {
			packer.PackString(stringValue)
		} else if boolValue, ok := value.(bool); ok {
			if boolValue {
				packer.PackString("true")
			} else {
				packer.PackString("false")
			}
		} else if jsonNumber, ok := value.(json.Number); ok {
			packer.PackString(jsonNumber.String())
		}
	case dc.PTBlob:
		if binData, ok := value.([]byte); ok {
			packer.PackString(string(binData))
		} else if stringValue, ok := value.(string); ok {
			packer.PackString(stringValue)
		}
	default:
		if array, ok := value.([]interface{}); ok {
			packer.Push()
			for _, v := range array {
				PackValue(packer, v, log)
			}
			packer.Pop()
		} else if doc, ok := value.(map[string]interface{}); ok {
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
					PackValue(packer, v, log)
				} else {
					// Maybe "_{element_number}" is the key instead.
					if name != "" {
						name = fmt.Sprintf("_%d", i)
						if v, ok := doc[name]; ok {
							PackValue(packer, v, log)
						}
					}
				}
			}
			packer.Pop()
		} else if value == nil {
			return
		} else {
			log.Warnf("Unknown value type in PackValue: %T with value: %v", value, value)
			if strValue := fmt.Sprintf("%v", value); strValue != "" {
				packer.PackString(strValue)
			}
		}
	}
}
func NewMySQLBackend(db *DatabaseServer, config Config) (bool, *MySQLBackend, error) {
	dbConn, err := sql.Open("mysql", config.Server)
	if err != nil {
		return false, nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = dbConn.ExecContext(ctx, fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", config.Database))
	if err != nil {
		return false, nil, fmt.Errorf("Failed to create database: %w", err)
	}

	dbConn.Close()

	dsnWithDatabaseName := fmt.Sprintf("%s%s", config.Server, config.Database)

	dbConn, err = sql.Open("mysql", dsnWithDatabaseName)
	if err != nil {
		return false, nil, err
	}

	dbConn.SetMaxOpenConns(25)
	dbConn.SetMaxIdleConns(25)
	dbConn.SetConnMaxLifetime(5 * time.Minute)

	if err := dbConn.PingContext(ctx); err != nil {
		return false, nil, fmt.Errorf("Failed to connect to database %s: %w", config.Database, err)
	}

	db.log.Infof("Successfully connected to MySQL database: %s", config.Database)

	backend := &MySQLBackend{
		db:     db,
		dbConn: dbConn,
	}

	// Create tables if it doesn't exist already.
	if err := backend.createTables(); err != nil {
		return false, nil, err
	}

	// Initialize globals if it doesn't exist already.
	if err := backend.initializeGlobals(); err != nil {
		return false, nil, err
	}

	return true, backend, nil
}

func (b *MySQLBackend) createTables() error {
	// Create globals table
	globalsDDL := `
		CREATE TABLE IF NOT EXISTS globals (
			_id VARCHAR(255) PRIMARY KEY,
			doid JSON
		)`

	_, err := b.dbConn.Exec(globalsDDL)
	if err != nil {
		return err
	}

	// Create objects table
	objectsDDL := `
		CREATE TABLE IF NOT EXISTS objects (
			_id INT UNSIGNED PRIMARY KEY,
			dclass VARCHAR(255) NOT NULL,
			fields JSON,
			INDEX idx_class (dclass)
		)`

	_, err = b.dbConn.Exec(objectsDDL)
	return err
}

func (b *MySQLBackend) initializeGlobals() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	err := b.dbConn.QueryRowContext(ctx, "SELECT COUNT(*) FROM globals WHERE _id = 'GLOBALS'").Scan(&count)
	if err != nil {
		return err
	}

	if count == 0 {
		globals := MySQLGlobals{
			ID: "GLOBALS",
			DoId: &MySQLGlobalsDoId{
				Monotonic: b.db.min,
				Free:      make([]Doid_t, 0),
			},
		}

		doidJSON, err := json.Marshal(globals.DoId)
		if err != nil {
			return err
		}

		_, err = b.dbConn.ExecContext(ctx,
			"INSERT INTO globals (_id, doid) VALUES ('GLOBALS', ?)",
			string(doidJSON))
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *MySQLBackend) AssignDoId() Doid_t {
	monotonicDoId := b.AssignDoIdMonotonic()
	if monotonicDoId != INVALID_DOID {
		return monotonicDoId
	}

	// TODO: AssignDoIdReuse
	return INVALID_DOID
}

func (b *MySQLBackend) AssignDoIdMonotonic() Doid_t {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := b.dbConn.BeginTx(ctx, nil)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to begin transaction: %s", err.Error())
		return INVALID_DOID
	}
	defer tx.Rollback()

	var doidJSON []byte
	err = tx.QueryRowContext(ctx,
		"SELECT doid FROM globals WHERE _id = 'GLOBALS' FOR UPDATE").Scan(&doidJSON)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to query globals: %s", err.Error())
		return INVALID_DOID
	}

	var globalsDoId MySQLGlobalsDoId
	err = json.Unmarshal(doidJSON, &globalsDoId)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to unmarshal globals: %s", err.Error())
		return INVALID_DOID
	}

	if globalsDoId.Monotonic > b.db.max {
		b.db.log.Errorf("AssignDoIdMonotonic: Monotonic doId %d exceeds maximum %d", globalsDoId.Monotonic, b.db.max)
		return INVALID_DOID
	}

	globalsDoId.Monotonic++
	updatedDoidJSON, err := json.Marshal(globalsDoId)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to marshal globals: %s", err.Error())
		return INVALID_DOID
	}

	_, err = tx.ExecContext(ctx,
		"UPDATE globals SET doid = ? WHERE _id = 'GLOBALS'",
		string(updatedDoidJSON))
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to update globals: %s", err.Error())
		return INVALID_DOID
	}

	err = tx.Commit()
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: Failed to commit transaction: %s", err.Error())
		return INVALID_DOID
	}

	return globalsDoId.Monotonic - 1
}

func (b *MySQLBackend) CreateStoredObject(dclass dc.DCClass, datas map[dc.DCField]dc.Vector,
	ctx uint32, sender Channel_t) {

	var doc map[string]interface{}

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

			if doc == nil {
				doc = make(map[string]interface{})
			}
			UnpackDataToDocument(unpacker, field.GetName(), doc, *b.db.log)

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
		doc = make(map[string]interface{})
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

	fieldsJSON, err := json.Marshal(doc)
	if err != nil {
		b.db.log.Errorf("Failed to marshal fields for object %d: %s", doId, err.Error())
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

	// Insert object into database
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = b.dbConn.ExecContext(queryCtx,
		"INSERT INTO objects (_id, dclass, fields) VALUES (?, ?, ?)",
		doId, dclass.GetName(), string(fieldsJSON))
	if err != nil {
		b.db.log.Errorf("Insertion of %s object failed: %s", dclass.GetName(), err.Error())
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

	b.db.log.Debugf("Successfully created new %s object with ID: %d", dclass.GetName(), doId)

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

func (b *MySQLBackend) GetStoredValues(doId Doid_t, fields []string, ctx uint32, sender Channel_t) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var obj MySQLStoredObject
	var fieldsRaw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		"SELECT _id, dclass, fields FROM objects WHERE _id = ?", doId).Scan(
		&obj.ID, &obj.Class, &fieldsRaw)
	if err != nil {
		if err == sql.ErrNoRows {
			b.db.log.Errorf("Object %d not found in database", doId)
		} else {
			b.db.log.Errorf("Failed to query object %d: %s", doId, err.Error())
		}

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

	dclass := core.DC.GetClassByName(obj.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", obj.Class, doId)

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

	// Unmarshal fields JSON
	fieldsMap := make(map[string]interface{})
	if fieldsRaw != nil {
		err = json.Unmarshal(fieldsRaw, &fieldsMap)
		if err != nil {
			b.db.log.Errorf("Failed to unmarshal fields for object %d: %s", doId, err.Error())
		}
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedData := map[string]dc.Vector{}
	for _, field := range fields {
		dcField := dclass.GetFieldByName(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, obj.Class)
			continue
		}

		if field == "DcObjectType" {
			// Return dclass type
			packedData[field] = dcField.ParseString("\"" + obj.Class + "\"")
			continue
		}

		value, ok := fieldsMap[field]
		if !ok {
			// Field not found, that's alright, continue on.
			continue
		}

		packer.BeginPack(dcField)
		PackValue(packer, value, *b.db.log)
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

func (b *MySQLBackend) SetStoredValues(doId Doid_t, packedValues map[string]dc.Vector) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var obj MySQLStoredObject
	var fieldsRaw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		"SELECT _id, dclass, fields FROM objects WHERE _id = ?", doId).Scan(
		&obj.ID, &obj.Class, &fieldsRaw)
	if err != nil {
		b.db.log.Errorf("Failed to retrieve object %d from database: %s", doId, err.Error())
		return
	}

	dclass := core.DC.GetClassByName(obj.Class)
	if dclass == dc.SwigcptrDCClass(0) {
		b.db.log.Errorf("Class %s for object %d does not exist!", obj.Class, doId)
		return
	}

	fieldsMap := make(map[string]interface{})
	if fieldsRaw != nil {
		err = json.Unmarshal(fieldsRaw, &fieldsMap)
		if err != nil {
			b.db.log.Errorf("Failed to unmarshal fields for object %d: %s", doId, err.Error())
			return
		}
	}

	DCLock.Lock()
	defer DCLock.Unlock()

	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	for field, value := range packedValues {
		if value.Size() == 0 {
			delete(fieldsMap, field)
			continue
		}

		dcField := dclass.GetFieldByName(field)
		if dcField == dc.SwigcptrDCField(0) {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, obj.Class)
			continue
		}

		unpacker.SetUnpackData(value)
		unpacker.BeginUnpack(dcField)
		b.db.log.Debugf("Beginning unpack field \"%s\"\n%s", field, DumpUnpacker(unpacker))

		UnpackDataToDocument(unpacker, field, fieldsMap, *b.db.log)
		if !unpacker.EndUnpack() {
			b.db.log.Errorf("Failed to unpack field \"%s\"! Update aborted.\n%s", field, DumpUnpacker(unpacker))
			for _, data := range packedValues {
				dc.DeleteVector(data)
			}
			return
		}
	}

	for _, data := range packedValues {
		dc.DeleteVector(data)
	}

	if len(fieldsMap) == 0 {
		b.db.log.Warnf("Nothing to do for update to object %s(%d).", obj.Class, doId)
		return
	}

	fieldsJSON, err := json.Marshal(fieldsMap)
	if err != nil {
		b.db.log.Errorf("Failed to marshal fields for object %d: %s", doId, err.Error())
		return
	}

	result, err := b.dbConn.ExecContext(queryCtx,
		"UPDATE objects SET fields = ? WHERE _id = ?", string(fieldsJSON), doId)
	if err != nil {
		b.db.log.Errorf("An error has occurred when updating %s(%d): %s", obj.Class, doId, err.Error())
		return
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		b.db.log.Errorf("Failed to get rows affected for object %d: %s", doId, err.Error())
		return
	}

	if rowsAffected > 0 {
		b.db.log.Debugf("Successfully updated object %s(%d)", obj.Class, doId)
	}
}
