//go:build !no_dbserver && no_yaml

package database

import "errors"

func NewYAMLBackend(db *DatabaseServer, config Config) (bool, DatabaseBackend, error) {
	return false, nil, errors.New("YAML backend is not compiled.  To use, remove the \"no_yaml\" build tag.  Halting.")
}
