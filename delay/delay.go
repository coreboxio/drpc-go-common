package delay

import (
	"time"
)

type DelayExecutor struct{}

func (d *DelayExecutor) Execute(delay time.Duration, fn func()) {
	go func() {
		time.Sleep(delay)
		fn()
	}()
}

var Executor = &DelayExecutor{}
