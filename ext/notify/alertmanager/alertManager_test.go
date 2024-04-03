package alertmanager_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/notify/alertmanager"
)

func TestAlertManager(t *testing.T) {
	projectName := "ss"
	namespaceName := "bb"
	jobName := scheduler.JobName("foo-job-spec")
	tnnt, _ := tenant.NewTenant(projectName, namespaceName)
	eventTime := time.Now()
	scheduledAt := eventTime.Add(-2 * time.Hour)
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
			assert.NotEmpty(t, payload.Data["project"])
			assert.NotEmpty(t, payload.Data["namespace"])
			assert.NotEmpty(t, payload.Data["job_name"])
			assert.NotEmpty(t, payload.Data["owner"])
			assert.NotEmpty(t, payload.Data["scheduled_at"])
			assert.NotEmpty(t, payload.Data["console_link"])
			assert.NotEmpty(t, payload.Data["dashboard"])
			assert.NotEmpty(t, payload.Data["airflow_logs"])

			w.WriteHeader(http.StatusOK)
		})
		mockServer := httptest.NewServer(httpHandler)
		defer mockServer.Close()

		err := alertmanager.RelayEvent(&scheduler.AlertAttrs{
			Owner:  "@data-batching",
			JobURN: "urn:optimus:project:job:project.namespace.job_name",
			Title:  "Optimus Job Alert",
			Status: scheduler.StatusFiring,
			JobEvent: &scheduler.Event{
				JobName:        jobName,
				Tenant:         tnnt,
				Type:           scheduler.SLAMissEvent,
				EventTime:      eventTime,
				OperatorName:   "bq2bq",
				Status:         "success",
				JobScheduledAt: scheduledAt,
				Values:         map[string]any{},
				SLAObjectList: []*scheduler.SLAObject{
					{
						JobName:        jobName,
						JobScheduledAt: scheduledAt,
					},
				},
			},
		}, mockServer.URL, alertManagerEndPoint, "dashboard_url", "data_console_url")
		assert.Nil(t, err)

		assert.Equal(t, reqRecorder.Code, http.StatusOK)
	})
}
