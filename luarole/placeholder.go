//go:build no_luarole

package luarole

import "otpgo/core"

func NewLuaRole(config core.Role) bool {
	return false
}
