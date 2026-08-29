//go:build !no_dbserver && no_postgres

package database

import "errors"

func NewPostgresBackend(db *DatabaseServer, config Config) (bool, DatabaseBackend, error) {
	return false, nil, errors.New("PostgreSQL backend is not compiled.  To use, remove the \"no_postgres\" build tag.  Halting.")
}
