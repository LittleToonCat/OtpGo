//go:build !no_dbserver && no_mongodb

package database

import "errors"

func NewMongoBackend(db *DatabaseServer, config Config) (bool, DatabaseBackend, error) {
	return false, nil, errors.New("MongoDB backend is not compiled.  To use, remove the \"no_mongodb\" build tag.  Halting.")
}
