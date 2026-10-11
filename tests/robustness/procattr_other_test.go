//go:build !linux

package robustness

import "syscall"

// dieWithParent has no portable equivalent outside Linux: a test that dies
// without its cleanup leaves its nodes running there.
func dieWithParent() *syscall.SysProcAttr { return nil }
