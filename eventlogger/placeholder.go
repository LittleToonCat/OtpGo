//go:build no_eventlogger

package eventlogger

import "otpgo/core"

func StartEventLogger(config core.Role) bool {
	return false
}
