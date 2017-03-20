package control

import (
	"errors"
)

var (
	TimeOutError = errors.New("timeout is already reached")
	FatalError   = errors.New("can't recover error")
)
