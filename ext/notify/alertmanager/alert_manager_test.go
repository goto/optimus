package alertmanager_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/ext/notify/alertmanager"
)

type mockAlertLogProvider struct {
	mock.Mock
}

func (m *mockAlertLogProvider) Insert(ctx context.Context, payload *alertmanager.AlertPayload) (uuid.UUID, bool, error) {
	args := m.Called(ctx, payload)
	return args.Get(0).(uuid.UUID), args.Bool(1), args.Error(2)
}

func (m *mockAlertLogProvider) UpdateStatus(ctx context.Context, recordID uuid.UUID, status alertmanager.AlertStatus, message string) error {
	args := m.Called(ctx, recordID, status, message)
	return args.Error(0)
}

func (m *mockAlertLogProvider) ShouldWarnOnDedup(alertType string) bool {
	args := m.Called(alertType)
	return args.Bool(0)
}

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
		am := alertmanager.New(ctx, log.NewNoop(), mockServer.URL, alertManagerEndPoint, "dashboard_url", "data_console_url", nil, alertmanager.AlertRules{})
		err := am.PrepareAndSendEvent(&alertmanager.AlertPayload{
			Project: projectName,
			LogTag:  jobName.String(),
			Data: map[string]interface{}{
				"project":   projectName,
				"namespace": "some-ns",
				"job_name":  jobName.String(),
				"owner":     "some-owner",
			},
			Template: alertmanager.OptimusChangeTemplate,
			Labels: map[string]string{
				"JobURN": "urn:optimus:project:job:project.namespace.job_name",
			},
			Endpoint: mockServer.URL + alertManagerEndPoint,
		})
		assert.Nil(t, err)

		assert.Equal(t, reqRecorder.Code, http.StatusOK)
	})

	t.Run("should skip alert for backfill job", func(t *testing.T) {
		ar := alertmanager.AlertRules{
			TemplatesToSkipDuringBackfills: []string{alertmanager.OptimusSLAAlertTemplate, alertmanager.OptimusOperatorSLAMissTemplate},
			BackfillLookBackPeriodInHours:  2,
		}
		am := alertmanager.New(context.Background(), log.NewNoop(), "http://alertmanager", alertManagerEndPoint, "dashboard_url", "data_console_url", nil, ar)

		tc := []struct {
			name         string
			alertPayload alertmanager.AlertPayload
			expectSkip   bool
		}{
			{
				name: "should not detected as backfill for non targeted template",
				alertPayload: alertmanager.AlertPayload{
					Template: alertmanager.OptimusFailureAlertTemplate,
				},
				expectSkip: false,
			},
			{
				name: "should not detected as backfil when job run scheduled at is zero value",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusSLAAlertTemplate,
					JobRunScheduledAt: time.Time{},
				},
				expectSkip: false,
			},
			{
				name: "should not detected as backfill when scheduled time within lookback period",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusSLAAlertTemplate,
					JobRunScheduledAt: time.Now().Add(-1 * time.Hour),
				},
				expectSkip: false,
			},
			{
				name: "should detected as backfill when scheduled time outside lookback period and using targeted template",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusSLAAlertTemplate,
					JobRunScheduledAt: time.Now().Add(-3 * time.Hour),
				},
				expectSkip: true,
			},
			{
				name: "should detected as backfill when dag run id belongs to a custom backfill and template is targeted",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusSLAAlertTemplate,
					JobRunScheduledAt: time.Now().Add(time.Hour), // future, custom backfills carry future scheduled_at
					DagRunID:          "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19",
				},
				expectSkip: true,
			},
			{
				name: "should detected as backfill for operator sla miss template on a custom backfill run",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusOperatorSLAMissTemplate,
					JobRunScheduledAt: time.Now().Add(time.Hour), // future, custom backfills carry future scheduled_at
					DagRunID:          "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19",
				},
				expectSkip: true,
			},
			{
				name: "should not detected as backfill for operator sla miss template on a scheduled run",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusOperatorSLAMissTemplate,
					JobRunScheduledAt: time.Now().Add(-1 * time.Hour), // within lookback
					DagRunID:          "scheduled__2026-10-19T10:00:00+00:00",
				},
				expectSkip: false,
			},
			{
				name: "should not detected as backfill for custom backfill run when template is not targeted",
				alertPayload: alertmanager.AlertPayload{
					Template: alertmanager.OptimusFailureAlertTemplate,
					DagRunID: "custom-backfill_9f7b0d4e-6b3a-4c11-8f2e-1d2b3c4d5e6f__2026-10-19",
				},
				expectSkip: false,
			},
			{
				name: "should not detected as backfill for a scheduled run with future scheduled time",
				alertPayload: alertmanager.AlertPayload{
					Template:          alertmanager.OptimusSLAAlertTemplate,
					JobRunScheduledAt: time.Now().Add(time.Hour),
					DagRunID:          "scheduled__2026-10-19T10:00:00+00:00",
				},
				expectSkip: false,
			},
		}

		for _, tc := range tc {
			t.Run(tc.name, func(t *testing.T) {
				skip := am.IsBackFill(&tc.alertPayload)
				assert.Equal(t, tc.expectSkip, skip)
			})
		}
	})

	// worker processing can be delayed up to eventBatchInterval (10s) when the
	// worker goroutine is in its default sleep, hence the generous timeouts.
	t.Run("should re-send deduplicated alert with WARNING severity when warn-on-dedup is enabled", func(t *testing.T) {
		sent := make(chan alertmanager.AlertPayload, 1)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var payload alertmanager.AlertPayload
			_ = json.NewDecoder(r.Body).Decode(&payload)
			sent <- payload
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		provider := new(mockAlertLogProvider)
		provider.On("Insert", mock.Anything, mock.Anything).Return(uuid.New(), true, nil)
		provider.On("ShouldWarnOnDedup", alertmanager.AlertTypePotentialSLABreach).Return(true)

		am := alertmanager.New(context.Background(), log.NewNoop(), mockServer.URL, "/endpoint", "dashboard_url", "data_console_url", provider, alertmanager.AlertRules{})
		am.SendPotentialSLABreach(&scheduler.PotentialSLABreachAlert{
			Team:         "data-eng",
			Project:      "sample_project",
			RootCauseJob: "root-cause-job",
			Reason:       scheduler.ReasonRunningLong,
			Severity:     alertmanager.CriticalSeverity,
		})

		select {
		case payload := <-sent:
			assert.Equal(t, alertmanager.WarningSeverity, payload.Labels[alertmanager.SeverityLabel])
			assert.NotContains(t, payload.Labels, alertmanager.EnvironmentLabel,
				"environment label must be dropped when severity is downgraded from CRITICAL")
		case <-time.After(15 * time.Second):
			t.Fatal("deduplicated alert was not re-sent through alert manager")
		}

		// the log row must keep its DEDUPLICATED status
		provider.AssertNotCalled(t, "UpdateStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("should not re-send deduplicated alert when warn-on-dedup is disabled", func(t *testing.T) {
		sent := make(chan alertmanager.AlertPayload, 1)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var payload alertmanager.AlertPayload
			_ = json.NewDecoder(r.Body).Decode(&payload)
			sent <- payload
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		provider := new(mockAlertLogProvider)
		provider.On("Insert", mock.Anything, mock.Anything).Return(uuid.New(), true, nil)
		provider.On("ShouldWarnOnDedup", alertmanager.AlertTypePotentialSLABreach).Return(false)

		am := alertmanager.New(context.Background(), log.NewNoop(), mockServer.URL, "/endpoint", "dashboard_url", "data_console_url", provider, alertmanager.AlertRules{})
		am.SendPotentialSLABreach(&scheduler.PotentialSLABreachAlert{
			Team:         "data-eng",
			Project:      "sample_project",
			RootCauseJob: "root-cause-job",
			Reason:       scheduler.ReasonRunningLong,
			Severity:     alertmanager.CriticalSeverity,
		})

		select {
		case payload := <-sent:
			t.Fatalf("deduplicated alert should have been skipped, got: %+v", payload)
		case <-time.After(time.Second):
		}
	})

	t.Run("should send non-deduplicated alert with its original severity", func(t *testing.T) {
		sent := make(chan alertmanager.AlertPayload, 1)
		mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var payload alertmanager.AlertPayload
			_ = json.NewDecoder(r.Body).Decode(&payload)
			sent <- payload
			w.WriteHeader(http.StatusOK)
		}))
		defer mockServer.Close()

		provider := new(mockAlertLogProvider)
		provider.On("Insert", mock.Anything, mock.Anything).Return(uuid.New(), false, nil)
		provider.On("ShouldWarnOnDedup", alertmanager.AlertTypePotentialSLABreach).Return(true)

		am := alertmanager.New(context.Background(), log.NewNoop(), mockServer.URL, "/endpoint", "dashboard_url", "data_console_url", provider, alertmanager.AlertRules{})
		am.SendPotentialSLABreach(&scheduler.PotentialSLABreachAlert{
			Team:         "data-eng",
			Project:      "sample_project",
			RootCauseJob: "root-cause-job",
			Reason:       scheduler.ReasonRunningLong,
			Severity:     alertmanager.CriticalSeverity,
		})

		select {
		case payload := <-sent:
			assert.Equal(t, alertmanager.CriticalSeverity, payload.Labels[alertmanager.SeverityLabel])
			assert.Equal(t, "production", payload.Labels[alertmanager.EnvironmentLabel])
		case <-time.After(15 * time.Second):
			t.Fatal("non-deduplicated alert was not sent")
		}
	})
}
