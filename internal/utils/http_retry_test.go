package utils

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryableHTTPStatus(t *testing.T) {
	assert.True(t, IsRetryableHTTPStatus(503))
	assert.False(t, IsRetryableHTTPStatus(404))
}

func TestIsRetryableTransportError(t *testing.T) {
	assert.True(t, IsRetryableTransportError(io.EOF))
	assert.True(t, IsRetryableTransportError(errors.New(`Post "http://airflow/dagRuns/list": EOF`)))
	assert.False(t, IsRetryableTransportError(context.Canceled))
	assert.True(t, IsRetryableTransportError(&net.OpError{Err: errors.New("connection reset by peer")}))
}
