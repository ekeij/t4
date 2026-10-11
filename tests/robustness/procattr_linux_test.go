package robustness

import "syscall"

// dieWithParent makes a node get SIGKILL when the test process dies, so a
// test that times out or panics does not leave its cluster running.
func dieWithParent() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
}
