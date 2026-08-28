package database

import (
	"context"
	"fmt"
	"time"
)

func referenceIndexName(ref Reference) string {
	return fmt.Sprintf("objref_%s_%s", ref.ClassName, ref.FieldName)
}

func referenceIndexDDL(ref Reference) string {
	name := referenceIndexName(ref)
	listExpr := fmt.Sprintf("fields->'%s'", ref.FieldName)
	scalarExpr := fmt.Sprintf("fields->>'%s'", ref.FieldName)
	if ref.Atomic {
		listExpr = fmt.Sprintf("fields->'%s'->0", ref.FieldName)
		scalarExpr = fmt.Sprintf("fields->'%s'->>0", ref.FieldName)
	}

	if ref.IsList {
		return fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS "%s" ON objects USING GIN ((%s) jsonb_path_ops)`,
			name, listExpr)
	}
	return fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS "%s" ON objects (((%s)::bigint)) WHERE dclass = '%s'`,
		name, scalarExpr, ref.ClassName)
}

func (b *PostgresBackend) ensureReferenceIndexes(reg *ReferenceRegistry) error {
	if reg.Empty() {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, ref := range reg.All() {
		ddl := referenceIndexDDL(ref)
		if _, err := b.dbConn.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("creating reference index for %s.%s: %w", ref.ClassName, ref.FieldName, err)
		}
		b.db.log.Infof("Ensured reference index %s (%s.%s -> %s)",
			referenceIndexName(ref), ref.ClassName, ref.FieldName, ref.Target.GetName())
	}
	return nil
}
