//go:build !no_dbserver && no_mysql

package database

import "errors"

func NewMySQLBackend(db *DatabaseServer, config Config) (bool, DatabaseBackend, error) {
	return false, nil, errors.New("MySQL backend is not compiled.  To use, remove the \"no_mysql\" build tag.  Halting.")
}
