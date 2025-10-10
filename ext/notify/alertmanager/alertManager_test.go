package alertmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/ext/notify/alertmanager"
)

func TestAlertManager(t *testing.T) {
	projectName := "ss"
	jobName := scheduler.JobName("foo-job-spec")
	alertManagerEndPoint := "/endpoint"

	t.Run("should send event to alert manager", func(t *testing.T) {
		reqRecorder := httptest.NewRecorder()
		httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Method, http.MethodPost)
			assert.Equal(t, r.Header.Get("Content-Type"), "application/json")

			assert.Equal(t, r.URL.String(), alertManagerEndPoint)

			var payload alertmanager.AlertPayload
			assert.Nil(t, json.NewDecoder(r.Body).Decode(&payload))

			// Check if the payload is properly formed
			assert.Equal(t, "optimus-change", payload.Template)
			assert.Equal(t, "urn:optimus:project:job:project.namespace.job_name", payload.Labels["JobURN"])
			assert.NotEmpty(t, payload.Data["project"])
			assert.NotEmpty(t, payload.Data["namespace"])
			assert.NotEmpty(t, payload.Data["job_name"])
			assert.NotEmpty(t, payload.Data["owner"])

			w.WriteHeader(http.StatusOK)
		})
		mockServer := httptest.NewServer(httpHandler)
		defer mockServer.Close()
		ctx := context.Background()
		am := alertmanager.New(ctx, log.NewNoop(), mockServer.URL, alertManagerEndPoint, "dashboard_url", "data_console_url", nil)
		err := am.PrepareAndSendEvent(&alertmanager.AlertPayload{
			Project: projectName,
			LogTag:  jobName.String(),
			Data: map[string]interface{}{
				"project":   projectName,
				"namespace": "some-ns",
				"job_name":  jobName.String(),
				"owner":     "some-owner",
			},
			Template: "optimus-change",
			Labels: map[string]string{
				"JobURN": "urn:optimus:project:job:project.namespace.job_name",
			},
			Endpoint: mockServer.URL + alertManagerEndPoint,
		})
		assert.Nil(t, err)

		assert.Equal(t, reqRecorder.Code, http.StatusOK)
	})
}
