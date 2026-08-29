//go:build !no_dbserver

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

func (b *MySQLBackend) CreateStoredObject(dclass *dc.DCClass, datas map[dc.DCField][]byte,
	ctx uint32, sender Channel_t) {

	var doc map[string]interface{}

	defaults := map[dc.DCField][]byte{}

	for i := 0; i < dclass.GetNumInheritedFields(); i++ {
		field := dclass.GetInheritedField(i)
		if field.IsDb() {
			if field.AsMolecularField() != nil {
				continue
			}

			data, ok := datas[field]
			if !ok {
				// Use default value instead if there is any.
				if field.HasDefaultValue() {
					data = field.GetDefaultValue()
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
	if dclass == nil {
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

	packer := dc.NewDCPacker()
	defer dc.DeleteDCPacker(packer)

	packedData := map[string][]byte{}
	for _, field := range fields {
		dcField := dclass.GetFieldByName(field)
		if dcField == nil {
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
			dg.AddUint16(uint16(len(packedValue)))
			dg.AddData(packedValue)
			dg.AddBool(true) // Found
		} else {
			dg.AddString("")
			dg.AddBool(false) // Not found
		}
	}
	b.db.RouteDatagram(dg)

}

func (b *MySQLBackend) SetStoredValues(doId Doid_t, packedValues map[string][]byte) {
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
	if dclass == nil {
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

	unpacker := dc.NewDCPacker()
	defer dc.DeleteDCPacker(unpacker)

	for field, value := range packedValues {
		if len(value) == 0 {
			delete(fieldsMap, field)
			continue
		}

		dcField := dclass.GetFieldByName(field)
		if dcField == nil {
			b.db.log.Errorf("Field %s for class %s does not exist!", field, obj.Class)
			continue
		}

		unpacker.SetUnpackData(value)
		unpacker.BeginUnpack(dcField)
		b.db.log.Debugf("Beginning unpack field \"%s\"\n%s", field, DumpUnpacker(unpacker))

		UnpackDataToDocument(unpacker, field, fieldsMap, *b.db.log)
		if !unpacker.EndUnpack() {
			b.db.log.Errorf("Failed to unpack field \"%s\"! Update aborted.\n%s", field, DumpUnpacker(unpacker))
			return
		}
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

func (b *MySQLBackend) fetchDoc(doId Doid_t) (string, map[string]interface{}, bool) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var class string
	var raw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		"SELECT dclass, fields FROM objects WHERE _id = ?", doId).Scan(&class, &raw)
	if err != nil {
		if err != sql.ErrNoRows {
			b.db.log.Errorf("fetchDoc(%d): %s", doId, err.Error())
		}
		return "", nil, false
	}

	doc := map[string]interface{}{}
	if raw != nil {
		if err := json.Unmarshal(raw, &doc); err != nil {
			b.db.log.Errorf("fetchDoc(%d): unmarshal: %s", doId, err.Error())
			return "", nil, false
		}
	}
	return class, doc, true
}

func (b *MySQLBackend) GetRelatedValues(req GetRelatedRequest, sender Channel_t) {
	parentClass, parentDoc, ok := b.fetchDoc(req.ParentDoId)
	if !ok {
		b.db.sendGetRelatedResp(sender, req.Context, 1, nil, nil, nil, nil)
		return
	}

	childIDs, code := b.db.relatedChildDOIDs(req, parentClass, parentDoc)
	if code != 0 {
		b.db.sendGetRelatedResp(sender, req.Context, code, nil, nil, nil, nil)
		return
	}

	var children []relatedChildPacked
	for _, id := range childIDs {
		cClass, cDoc, found := b.fetchDoc(id)
		if !found || cClass != req.TargetClass {
			b.db.log.Warnf("GetRelatedValues(%d): referenced %s %d missing or wrong class",
				req.ParentDoId, req.TargetClass, id)
			continue
		}
		children = append(children, relatedChildPacked{
			doId:   id,
			values: packDocFieldsJSON(b.db.log, req.TargetClass, req.TargetFields, cDoc),
		})
	}

	parentValues := packDocFieldsJSON(b.db.log, parentClass, req.ParentFields, parentDoc)
	b.db.sendGetRelatedResp(sender, req.Context, 0, req.ParentFields, parentValues, req.TargetFields, children)
}

func (b *MySQLBackend) DeleteStoredObject(doId Doid_t) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := b.dbConn.ExecContext(queryCtx, "DELETE FROM objects WHERE _id = ?", doId)
	if err != nil {
		b.db.log.Errorf("DeleteStoredObject(%d): %s", doId, err.Error())
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		b.db.log.Warnf("DeleteStoredObject(%d): object not found", doId)
		return
	}
	b.db.log.Debugf("Deleted object %d", doId)
}
