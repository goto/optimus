package airflow //nolint:testpackage

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/config"
)

func TestClientAirflowInvokeRetriesTransientErrors(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			hj, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(t, err)
			conn.Close()
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)

	client := NewAirflowClient(config.AirflowClientConfig{RetryMax: 3, RetryBackoffMs: 1})
	body, err := client.Invoke(context.Background(), airflowRequest{
		path:   "test",
		method: http.MethodGet,
	}, testSchedulerAuth(srv.URL))
	require.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(body))
	assert.Equal(t, int32(3), attempts.Load())
}

func TestClientAirflowInvokeRetriesRetryableHTTPStatus(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(srv.Close)

	client := NewAirflowClient(config.AirflowClientConfig{RetryMax: 2, RetryBackoffMs: 1})
	body, err := client.Invoke(context.Background(), airflowRequest{
		path:   "test",
		method: http.MethodGet,
	}, testSchedulerAuth(srv.URL))
	require.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(body))
	assert.Equal(t, int32(2), attempts.Load())
}

func TestClientAirflowInvokeDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	client := NewAirflowClient(config.AirflowClientConfig{RetryMax: 3, RetryBackoffMs: 1})
	_, err := client.Invoke(context.Background(), airflowRequest{
		path:   "test",
		method: http.MethodGet,
	}, testSchedulerAuth(srv.URL))
	require.Error(t, err)
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusNotFound))
	assert.Equal(t, int32(1), attempts.Load())
}

func testSchedulerAuth(serverURL string) SchedulerAuth {
	host := strings.TrimPrefix(strings.TrimPrefix(serverURL, "https://"), "http://")
	return SchedulerAuth{host: host, token: "user:pass"}
}
