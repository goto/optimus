package utils_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/internal/utils"
)

func TestIsRetryableHTTPStatus(t *testing.T) {
	assert.True(t, utils.IsRetryableHTTPStatus(503))
	assert.False(t, utils.IsRetryableHTTPStatus(404))
}

func TestIsRetryableTransportError(t *testing.T) {
	assert.True(t, utils.IsRetryableTransportError(io.EOF))
	assert.True(t, utils.IsRetryableTransportError(errors.New(`Post "http://airflow/dagRuns/list": EOF`)))
	assert.False(t, utils.IsRetryableTransportError(context.Canceled))
	assert.True(t, utils.IsRetryableTransportError(&net.OpError{Err: errors.New("connection reset by peer")}))
}

func TestIsRetryableClientError(t *testing.T) {
	assert.True(t, utils.IsRetryableClientError(errors.New(`status code received 502 on calling http://airflow`)))
	assert.False(t, utils.IsRetryableClientError(errors.New("internal error")))
}
