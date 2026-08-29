//go:build no_stateserver

package stateserver

import "otpgo/core"

func NewStateServer(config core.Role) bool {
	return false
}

func NewDatabaseStateServer(config core.Role) bool {
	return false
}
