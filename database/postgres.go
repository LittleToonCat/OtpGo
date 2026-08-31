//go:build !no_dbserver && !no_postgres

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"otpgo/core"
	. "otpgo/util"
	"strings"
	"time"

	"otpgo/dc"

	"github.com/lib/pq"
)

type PostgresGlobalsDoId struct {
	Monotonic Doid_t   `json:"monotonic"`
	Free      []Doid_t `json:"free"`
}

type PostgresStoredObject struct {
	ID     int64
	Class  string
	Fields map[string]interface{}
}

type PostgresBackend struct {
	db     *DatabaseServer
	dbConn *sql.DB
}

func NewPostgresBackend(db *DatabaseServer, config Config) (bool, *PostgresBackend, error) {
	maintenanceDSN, targetDSN, dbName, err := postgresConnStrings(config.Server, config.Database)
	if err != nil {
		return false, nil, err
	}

	if err := ensurePostgresDatabase(maintenanceDSN, dbName, db); err != nil {
		return false, nil, err
	}

	dbConn, err := sql.Open("postgres", targetDSN)
	if err != nil {
		return false, nil, err
	}

	dbConn.SetMaxOpenConns(25)
	dbConn.SetMaxIdleConns(25)
	dbConn.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := dbConn.PingContext(ctx); err != nil {
		return false, nil, fmt.Errorf("Failed to connect to Postgres: %w", err)
	}

	db.log.Infof("Successfully connected to Postgres database: %s", dbName)

	backend := &PostgresBackend{db: db, dbConn: dbConn}

	if err := backend.createTables(); err != nil {
		return false, nil, err
	}
	if err := backend.initializeGlobals(); err != nil {
		return false, nil, err
	}
	if err := backend.ensureReferenceIndexes(db.references); err != nil {
		return false, nil, err
	}

	return true, backend, nil
}

func postgresConnStrings(server, database string) (maintenance, target, dbName string, err error) {
	server = strings.TrimSpace(server)
	if server == "" {
		return "", "", "", fmt.Errorf("postgres: 'server' connection string is required")
	}

	if strings.HasPrefix(server, "postgres://") || strings.HasPrefix(server, "postgresql://") {
		u, perr := url.Parse(server)
		if perr != nil {
			return "", "", "", fmt.Errorf("postgres: invalid server URL: %w", perr)
		}
		dbName = database
		if dbName == "" {
			dbName = strings.TrimPrefix(u.Path, "/")
		}
		if dbName == "" {
			return "", "", "", fmt.Errorf("postgres: no database name in server URL or 'database' config")
		}
		tgt := *u
		tgt.Path = "/" + dbName
		maint := *u
		maint.Path = "/postgres"
		return maint.String(), tgt.String(), dbName, nil
	}

	dbName = database
	if dbName == "" {
		for _, tok := range strings.Fields(server) {
			if strings.HasPrefix(tok, "dbname=") {
				dbName = strings.TrimPrefix(tok, "dbname=")
			}
		}
	}
	if dbName == "" {
		return "", "", "", fmt.Errorf("postgres: no dbname in 'server' string or 'database' config")
	}
	return server + " dbname=postgres", server + " dbname=" + dbName, dbName, nil
}

func ensurePostgresDatabase(maintenanceDSN, dbName string, db *DatabaseServer) error {
	admin, err := sql.Open("postgres", maintenanceDSN)
	if err != nil {
		return err
	}
	defer admin.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var exists bool
	if err := admin.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)`, dbName).Scan(&exists); err != nil {
		return fmt.Errorf("postgres: checking for database %q: %w", dbName, err)
	}
	if exists {
		return nil
	}

	if _, err := admin.ExecContext(ctx,
		fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(dbName))); err != nil {
		if pgErr, ok := err.(*pq.Error); ok && pgErr.Code == "42P04" {
			return nil
		}
		return fmt.Errorf("postgres: creating database %q: %w", dbName, err)
	}

	db.log.Infof("Created Postgres database: %s", dbName)
	return nil
}

func (b *PostgresBackend) createTables() error {
	if _, err := b.dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS globals (
			_id  TEXT PRIMARY KEY,
			doid JSONB
		)`); err != nil {
		return err
	}

	if _, err := b.dbConn.Exec(`
		CREATE TABLE IF NOT EXISTS objects (
			_id    BIGINT PRIMARY KEY,
			dclass TEXT NOT NULL,
			fields JSONB
		)`); err != nil {
		return err
	}

	_, err := b.dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_objects_class ON objects (dclass)`)
	return err
}

func (b *PostgresBackend) initializeGlobals() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doidJSON, err := json.Marshal(&PostgresGlobalsDoId{Monotonic: b.db.min, Free: make([]Doid_t, 0)})
	if err != nil {
		return err
	}

	_, err = b.dbConn.ExecContext(ctx,
		`INSERT INTO globals (_id, doid) VALUES ('GLOBALS', $1) ON CONFLICT (_id) DO NOTHING`,
		string(doidJSON))
	return err
}

func (b *PostgresBackend) AssignDoId() Doid_t {
	if id := b.AssignDoIdMonotonic(); id != INVALID_DOID {
		return id
	}
	// TODO: AssignDoIdReuse
	return INVALID_DOID
}

func (b *PostgresBackend) AssignDoIdMonotonic() Doid_t {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tx, err := b.dbConn.BeginTx(ctx, nil)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: begin transaction: %s", err.Error())
		return INVALID_DOID
	}
	defer tx.Rollback()

	var doidJSON []byte
	if err = tx.QueryRowContext(ctx,
		`SELECT doid FROM globals WHERE _id = 'GLOBALS' FOR UPDATE`).Scan(&doidJSON); err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: query globals: %s", err.Error())
		return INVALID_DOID
	}

	var g PostgresGlobalsDoId
	if err = json.Unmarshal(doidJSON, &g); err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: unmarshal globals: %s", err.Error())
		return INVALID_DOID
	}

	if g.Monotonic > b.db.max {
		b.db.log.Errorf("AssignDoIdMonotonic: monotonic doId %d exceeds maximum %d", g.Monotonic, b.db.max)
		return INVALID_DOID
	}

	g.Monotonic++
	updated, err := json.Marshal(&g)
	if err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: marshal globals: %s", err.Error())
		return INVALID_DOID
	}

	if _, err = tx.ExecContext(ctx,
		`UPDATE globals SET doid = $1 WHERE _id = 'GLOBALS'`, string(updated)); err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: update globals: %s", err.Error())
		return INVALID_DOID
	}

	if err = tx.Commit(); err != nil {
		b.db.log.Errorf("AssignDoIdMonotonic: commit: %s", err.Error())
		return INVALID_DOID
	}

	return g.Monotonic - 1
}

func (b *PostgresBackend) createErrorResp(sender Channel_t, ctx uint32, code uint8) {
	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
	dg.AddUint32(ctx)
	dg.AddUint8(code)
	dg.AddDoid(INVALID_DOID)
	b.db.RouteDatagram(dg)
}

func (b *PostgresBackend) CreateStoredObject(dclass *dc.DCClass, datas map[dc.DCField][]byte,
	ctx uint32, sender Channel_t) {

	// Referential integrity: reject before allocating a doId.
	if err := b.checkReferentialIntegrity(dclass.GetName(), fieldNameMap(dclass, datas)); err != nil {
		b.db.log.Errorf("CreateStoredObject: %s", err.Error())
		b.createErrorResp(sender, ctx, 1)
		return
	}

	doc := make(map[string]interface{})

	for i := 0; i < dclass.GetNumInheritedFields(); i++ {
		field := dclass.GetInheritedField(i)
		if !field.IsDb() || field.AsMolecularField() != nil {
			continue
		}

		data, ok := datas[field]
		if !ok {
			if !field.HasDefaultValue() {
				continue
			}
			data = field.GetDefaultValue()
		}

		unpacker := dc.NewDCPacker()
		unpacker.SetUnpackData(data)
		unpacker.BeginUnpack(field)
		UnpackDataToDocument(unpacker, field.GetName(), doc, *b.db.log)
		ok = unpacker.EndUnpack()
		dc.DeleteDCPacker(unpacker)
		if !ok {
			b.db.log.Errorf("Failed to unpack field %q!", field.GetName())
			b.createErrorResp(sender, ctx, 1)
			return
		}
	}

	doId := b.AssignDoId()
	if doId == INVALID_DOID {
		b.db.log.Error("Unable to assign a doId!")
		b.createErrorResp(sender, ctx, 1)
		return
	}

	fieldsJSON, err := json.Marshal(doc)
	if err != nil {
		b.db.log.Errorf("Failed to marshal fields for object %d: %s", doId, err.Error())
		b.createErrorResp(sender, ctx, 1)
		return
	}

	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err = b.dbConn.ExecContext(queryCtx,
		`INSERT INTO objects (_id, dclass, fields) VALUES ($1, $2, $3)`,
		doId, dclass.GetName(), string(fieldsJSON)); err != nil {
		b.db.log.Errorf("Insertion of %s object failed: %s", dclass.GetName(), err.Error())
		b.createErrorResp(sender, ctx, 1)
		return
	}

	b.db.log.Debugf("Successfully created new %s object with ID: %d", dclass.GetName(), doId)

	dg := NewDatagram()
	dg.AddServerHeader(sender, b.db.control, DBSERVER_CREATE_STORED_OBJECT_RESP)
	dg.AddUint32(ctx)
	dg.AddUint8(0)
	dg.AddDoid(doId)
	b.db.RouteDatagram(dg)
}

func (b *PostgresBackend) GetStoredValues(doId Doid_t, fields []string, ctx uint32, sender Channel_t) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sendErr := func(code uint8) {
		dg := NewDatagram()
		dg.AddServerHeader(sender, b.db.control, DBSERVER_GET_STORED_VALUES_RESP)
		dg.AddUint32(ctx)
		dg.AddDoid(doId)
		dg.AddUint16(uint16(len(fields)))
		for _, field := range fields {
			dg.AddString(field)
		}
		dg.AddUint8(code)
		b.db.RouteDatagram(dg)
	}

	var obj PostgresStoredObject
	var fieldsRaw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		`SELECT _id, dclass, fields FROM objects WHERE _id = $1`, doId).Scan(
		&obj.ID, &obj.Class, &fieldsRaw)
	if err != nil {
		if err == sql.ErrNoRows {
			b.db.log.Errorf("Object %d not found in database", doId)
		} else {
			b.db.log.Errorf("Failed to query object %d: %s", doId, err.Error())
		}
		sendErr(1)
		return
	}

	dclass := core.DC.GetClassByName(obj.Class)
	if dclass == nil {
		b.db.log.Errorf("Class %s for object %d does not exist!", obj.Class, doId)
		sendErr(2)
		return
	}

	fieldsMap := make(map[string]interface{})
	if fieldsRaw != nil {
		if err = json.Unmarshal(fieldsRaw, &fieldsMap); err != nil {
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
			packedData[field] = dcField.ParseString("\"" + obj.Class + "\"")
			continue
		}

		value, ok := fieldsMap[field]
		if !ok {
			continue
		}

		packer.BeginPack(dcField)
		PackValue(packer, value, *b.db.log)
		if !packer.EndPack() {
			b.db.log.Errorf("Error has occurred when packing field %q", field)
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
	dg.AddUint8(0)
	for _, field := range fields {
		if packedValue, ok := packedData[field]; ok {
			dg.AddUint16(uint16(len(packedValue)))
			dg.AddData(packedValue)
			dg.AddBool(true)
		} else {
			dg.AddString("")
			dg.AddBool(false)
		}
	}
	b.db.RouteDatagram(dg)
}

func (b *PostgresBackend) SetStoredValues(doId Doid_t, packedValues map[string][]byte) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var obj PostgresStoredObject
	var fieldsRaw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		`SELECT _id, dclass, fields FROM objects WHERE _id = $1`, doId).Scan(
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

	if err := b.checkReferentialIntegrity(obj.Class, packedValues); err != nil {
		b.db.log.Errorf("SetStoredValues(%d): %s - update aborted", doId, err.Error())
		return
	}

	fieldsMap := make(map[string]interface{})
	if fieldsRaw != nil {
		if err = json.Unmarshal(fieldsRaw, &fieldsMap); err != nil {
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
		UnpackDataToDocument(unpacker, field, fieldsMap, *b.db.log)
		if !unpacker.EndUnpack() {
			b.db.log.Errorf("Failed to unpack field %q! Update aborted.", field)
			return
		}
	}

	fieldsJSON, err := json.Marshal(fieldsMap)
	if err != nil {
		b.db.log.Errorf("Failed to marshal fields for object %d: %s", doId, err.Error())
		return
	}

	if _, err = b.dbConn.ExecContext(queryCtx,
		`UPDATE objects SET fields = $1 WHERE _id = $2`, string(fieldsJSON), doId); err != nil {
		b.db.log.Errorf("An error has occurred when updating %s(%d): %s", obj.Class, doId, err.Error())
		return
	}

	b.db.log.Debugf("Successfully updated object %s(%d)", obj.Class, doId)
}

func (b *PostgresBackend) GetRelatedValues(req GetRelatedRequest, sender Channel_t) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var parentClass string
	var parentRaw []byte
	err := b.dbConn.QueryRowContext(queryCtx,
		`SELECT dclass, fields FROM objects WHERE _id = $1`, req.ParentDoId).Scan(&parentClass, &parentRaw)
	if err != nil {
		if err != sql.ErrNoRows {
			b.db.log.Errorf("GetRelatedValues(%d): %s", req.ParentDoId, err.Error())
		}
		b.db.sendGetRelatedResp(sender, req.Context, 1, nil, nil, nil, nil)
		return
	}

	parentDoc := map[string]interface{}{}
	if parentRaw != nil {
		if err = json.Unmarshal(parentRaw, &parentDoc); err != nil {
			b.db.log.Errorf("GetRelatedValues(%d): unmarshal parent: %s", req.ParentDoId, err.Error())
			b.db.sendGetRelatedResp(sender, req.Context, 1, nil, nil, nil, nil)
			return
		}
	}

	fieldName, _, _, elemIndex, atomic, ok := resolveRelationField(b.db.references, parentClass, req.RelationField, req.TargetClass)
	if !ok {
		b.db.log.Warnf("GetRelatedValues(%d): no relationship from %s to %s (field %q)",
			req.ParentDoId, parentClass, req.TargetClass, req.RelationField)
		b.db.sendGetRelatedResp(sender, req.Context, 2, nil, nil, nil, nil)
		return
	}

	childIDs := docChildDOIDs(parentDoc[fieldName], elemIndex, atomic)

	var children []relatedChildPacked
	if len(childIDs) > 0 {
		ids := make([]int64, len(childIDs))
		for i, d := range childIDs {
			ids[i] = int64(d)
		}

		rows, err := b.dbConn.QueryContext(queryCtx,
			`SELECT _id, fields FROM objects WHERE _id = ANY($1) AND dclass = $2`,
			pq.Array(ids), req.TargetClass)
		if err != nil {
			b.db.log.Errorf("GetRelatedValues(%d): child query: %s", req.ParentDoId, err.Error())
			b.db.sendGetRelatedResp(sender, req.Context, 1, nil, nil, nil, nil)
			return
		}

		childDocs := make(map[Doid_t]map[string]interface{}, len(ids))
		for rows.Next() {
			var id int64
			var raw []byte
			if err := rows.Scan(&id, &raw); err != nil {
				rows.Close()
				b.db.log.Errorf("GetRelatedValues(%d): scan child: %s", req.ParentDoId, err.Error())
				b.db.sendGetRelatedResp(sender, req.Context, 1, nil, nil, nil, nil)
				return
			}
			doc := map[string]interface{}{}
			if raw != nil {
				_ = json.Unmarshal(raw, &doc)
			}
			childDocs[Doid_t(id)] = doc
		}
		rows.Close()
		for _, id := range childIDs {
			doc, present := childDocs[id]
			if !present {
				b.db.log.Warnf("GetRelatedValues(%d): referenced %s %d missing or wrong class",
					req.ParentDoId, req.TargetClass, id)
				continue
			}
			children = append(children, relatedChildPacked{
				doId:   id,
				values: packDocFieldsJSON(b.db.log, req.TargetClass, req.TargetFields, doc),
			})
		}
	}

	parentValues := packDocFieldsJSON(b.db.log, parentClass, req.ParentFields, parentDoc)
	b.db.sendGetRelatedResp(sender, req.Context, 0, req.ParentFields, parentValues, req.TargetFields, children)
}

func (b *PostgresBackend) DeleteStoredObject(doId Doid_t) {
	queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := b.dbConn.ExecContext(queryCtx, `DELETE FROM objects WHERE _id = $1`, doId)
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

func (b *PostgresBackend) checkReferentialIntegrity(className string, packedValues map[string][]byte) error {
	if !b.db.enforceRefs || b.db.references.Empty() {
		return nil
	}

	for _, ref := range b.db.references.For(className) {
		packed, ok := packedValues[ref.FieldName]
		if !ok || len(packed) == 0 {
			continue
		}
		doids, err := b.db.references.ExtractDOIDs(ref, packed)
		if err != nil {
			return err
		}
		if len(doids) == 0 {
			continue
		}

		ids := make([]int64, len(doids))
		for i, d := range doids {
			ids[i] = int64(d)
		}

		queryCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		rows, err := b.dbConn.QueryContext(queryCtx,
			`SELECT _id, dclass FROM objects WHERE _id = ANY($1)`, pq.Array(ids))
		if err != nil {
			cancel()
			return fmt.Errorf("integrity query for %s.%s: %w", className, ref.FieldName, err)
		}

		found := make(map[int64]string, len(ids))
		for rows.Next() {
			var id int64
			var cls string
			if err := rows.Scan(&id, &cls); err != nil {
				rows.Close()
				cancel()
				return err
			}
			found[id] = cls
		}
		rows.Close()
		cancel()

		for _, id := range ids {
			cls, present := found[id]
			if !present {
				return fmt.Errorf("%s.%s references missing object %d", className, ref.FieldName, id)
			}
			if !classMatches(core.DC.GetClassByName(cls), ref.Target) {
				return fmt.Errorf("%s.%s references object %d of class %s, expected %s",
					className, ref.FieldName, id, cls, ref.Target.GetName())
			}
		}
	}
	return nil
}

func fieldNameMap(dclass *dc.DCClass, datas map[dc.DCField][]byte) map[string][]byte {
	out := make(map[string][]byte, len(datas))
	for f, v := range datas {
		out[f.GetName()] = v
	}
	return out
}

func classMatches(actual, target *dc.DCClass) bool {
	if actual == nil || target == nil {
		return false
	}
	if actual.GetNumber() == target.GetNumber() {
		return true
	}
	for i := 0; i < actual.GetNumParents(); i++ {
		if classMatches(actual.GetParent(i), target) {
			return true
		}
	}
	return false
}
