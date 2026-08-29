//go:build no_clientagent

package clientagent

import "otpgo/core"

func NewClientAgent(config core.Role) bool {
	return false
}
