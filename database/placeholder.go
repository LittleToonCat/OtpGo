//go:build no_dbserver

package database

import "otpgo/core"

func NewDatabaseServer(config core.Role) bool {
	return false
}
