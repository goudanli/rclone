//go:build windows || plan9
// +build windows plan9

package atexit

import (
	"os"
	"syscall"

	"github.com/rclone/rclone/lib/exitcode"
)

var exitSignals = []os.Signal{os.Interrupt, syscall.SIGKILL, syscall.SIGINT, syscall.SIGTERM}

func exitCode(_ os.Signal) int {
	return exitcode.UncategorizedError
}
