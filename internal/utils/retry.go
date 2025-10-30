package utils

import (
	"fmt"
	"time"

	"github.com/goto/salt/log"
)

// Retry retries the given function f up to retryMax times with exponential backoff starting at retryBackoffMs milliseconds.
func Retry(l log.Logger, retryMax int, retryBackoffMs int64, f func() error) error {
	var err error
	sleepTime := int64(1)

	for i := range retryMax {
		err = f()
		if err == nil {
			return nil
		}

		l.Warn(fmt.Sprintf("retry: %d, error: %v", i, err))
		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime*retryBackoffMs) * time.Millisecond)
	}

	return err
}
