package webhook // nolint: testpackage
import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

func TestWebhook(t *testing.T) {
	projectName := "ss"
	namespaceName := "bb"
	jobDestinationTableURN := "project-dest-table"
	jobName := scheduler.JobName("foo-job-spec")
	tnnt, _ := tenant.NewTenant(projectName, namespaceName)
	eventTime := time.Now()
	scheduledAt := eventTime.Add(-2 * time.Hour)
	t.Run("should send webhook to user url successfully", func(t *testing.T) {
		muxRouter := http.NewServeMux()
		server := httptest.NewServer(muxRouter)
		defer server.Close()
		muxRouter.HandleFunc("/users/webhook_end_point", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				Ok bool `json:"ok"`
			}{
				Ok: true,
			})
			rw.Write(response)
		})

		var sendErrors []error
		ctx, cancel := context.WithCancel(context.Background())
		client := NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
		)

		err := client.Notify(context.Background(), scheduler.WebhookAttrs{
			Owner: "",
			JobEvent: &scheduler.Event{
				JobName:        jobName,
				Tenant:         tnnt,
				Type:           scheduler.SLAMissEvent,
				EventTime:      eventTime,
				OperatorName:   "bq2bq",
				Status:         "success",
				JobScheduledAt: scheduledAt,
				Values:         map[string]any{},
				SLAObjectList:  nil,
			},
			Meta: &scheduler.JobRunMeta{
				Labels: map[string]string{
					"label1": "cohort1",
					"label2": "cohort2",
				},
				DestinationURN: jobDestinationTableURN,
			},
			Route: server.URL,
			Headers: map[string]string{
				"auth": "compiled_headers",
			},
		})

		assert.Nil(t, err)
		assert.Nil(t, sendErrors)
		cancel()
		err = client.Close()
		assert.Nil(t, err)
	})
	t.Run("should log wehook failure errors", func(t *testing.T) {
		muxRouter := http.NewServeMux()
		server := httptest.NewServer(muxRouter)
		defer server.Close()
		muxRouter.HandleFunc("/users/webhook_end_point", func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Set("Content-Type", "application/json")
			response, _ := json.Marshal(struct {
				Ok bool `json:"ok"`
			}{
				Ok: true,
			})
			rw.Write(response)
		})

		var sendErrors []error
		ctx, cancel := context.WithCancel(context.Background())
		client := NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
				assert.True(t, true, len(sendErrors) > 0)
			},
		)

		err := client.Notify(context.Background(), scheduler.WebhookAttrs{
			Owner: "",
			JobEvent: &scheduler.Event{
				JobName:        jobName,
				Tenant:         tnnt,
				Type:           scheduler.SLAMissEvent,
				EventTime:      eventTime,
				OperatorName:   "bq2bq",
				Status:         "success",
				JobScheduledAt: scheduledAt,
				Values:         map[string]any{},
				SLAObjectList:  nil,
			},
			Meta: &scheduler.JobRunMeta{
				Labels: map[string]string{
					"label1": "cohort1",
					"label2": "cohort2",
				},
				DestinationURN: jobDestinationTableURN,
			},
			Route: server.URL,
			Headers: map[string]string{
				"auth": "compiled_headers",
			},
		})

		assert.Nil(t, err)
		cancel()
		err = client.Close()
		assert.Nil(t, err)
	})
}
