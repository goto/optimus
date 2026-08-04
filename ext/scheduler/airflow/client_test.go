// This test lives in package airflow rather than airflow_test because the Client seam takes an
// unexported airflowRequest, so a stub client cannot be written from outside the package.
//
//nolint:testpackage // must be in-package to implement Client, whose argument is unexported
package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

func TestBuildEventLogQuery(t *testing.T) {
	tnnt, _ := tenant.NewTenant("proj", "ns")
	after := time.Date(2026, 7, 20, 12, 30, 0, 0, time.UTC)
	before := time.Date(2026, 7, 20, 13, 0, 0, 0, time.UTC)

	t.Run("sets every populated filter and omits the rest", func(t *testing.T) {
		query := buildEventLogQuery(scheduler.AuditEventFilter{
			Tenant:         tnnt,
			DagID:          "a-job",
			RunID:          "manual__2026-07-20T13:00:00+00:00",
			TaskID:         "a-task",
			After:          after,
			Before:         before,
			IncludedEvents: []string{"clear", "dagrun_clear"},
			ExcludedEvents: []string{"trigger"},
			Limit:          50,
		})

		values, err := url.ParseQuery(query)
		require.NoError(t, err)
		assert.Equal(t, "a-job", values.Get("dag_id"))
		assert.Equal(t, "manual__2026-07-20T13:00:00+00:00", values.Get("run_id"))
		assert.Equal(t, "a-task", values.Get("task_id"))
		assert.Equal(t, "2026-07-20T12:30:00Z", values.Get("after"))
		assert.Equal(t, "2026-07-20T13:00:00Z", values.Get("before"))
		// Airflow expects one comma separated value, not repeated parameters.
		assert.Equal(t, "clear,dagrun_clear", values.Get("included_events"))
		assert.Equal(t, "trigger", values.Get("excluded_events"))
		assert.Equal(t, "50", values.Get("limit"))
		// Newest first, so a caller can stop at the first row at or before the run's start.
		assert.Equal(t, "-when", values.Get("order_by"))
	})

	t.Run("omits empty and zero filters entirely", func(t *testing.T) {
		query := buildEventLogQuery(scheduler.AuditEventFilter{Tenant: tnnt})

		values, err := url.ParseQuery(query)
		require.NoError(t, err)
		for _, absent := range []string{"dag_id", "run_id", "task_id", "after", "before", "included_events", "excluded_events", "limit"} {
			_, present := values[absent]
			assert.False(t, present, "%s should be omitted when unset", absent)
		}
		assert.Equal(t, "-when", values.Get("order_by"))
	})
}

func TestGetEventLogs(t *testing.T) {
	logger := log.NewNoop()
	tnnt, _ := tenant.NewTenant("proj", "ns")

	t.Run("maps a bulk clear row that carries no dag context", func(t *testing.T) {
		// This is what /taskinstance/list/ and /dagrun/list/ produce: dag_id, task_id and run_id
		// are all null, and the only extra detail is an opaque row id.
		body := `{"event_logs":[{"event_log_id":11,"when":"2026-07-20T12:58:00+00:00","event":"action_clear",` +
			`"owner":"grace","dag_id":null,"task_id":null,"run_id":null,"execution_date":null,` +
			`"extra":"{\"rowid\": \"1234\"}"}],"total_entries":1}`

		sch := schedulerWithClient(logger, &stubClient{response: []byte(body)})

		events, err := sch.GetEventLogs(context.Background(), scheduler.AuditEventFilter{Tenant: tnnt})
		require.NoError(t, err)
		require.Len(t, events, 1)

		assert.Equal(t, int64(11), events[0].EventLogID)
		assert.Equal(t, "action_clear", events[0].Event)
		assert.Equal(t, "grace", events[0].Owner)
		assert.Empty(t, events[0].DagID)
		assert.Empty(t, events[0].RunID)
		assert.Nil(t, events[0].ExecutionDate)
		assert.Equal(t, `{"rowid": "1234"}`, events[0].Extra)
		assert.False(t, events[0].HasDagContext(), "a row with no dag_id cannot be matched exactly")
	})

	t.Run("maps a grid page clear that names the dag run", func(t *testing.T) {
		body := `{"event_logs":[{"event_log_id":42,"when":"2026-07-20T12:59:00+00:00","event":"dagrun_clear",` +
			`"owner":"dave","dag_id":"a-job","task_id":null,"run_id":"manual__2026-07-20T13:00:00+00:00",` +
			`"execution_date":"2026-07-20T12:00:00+00:00","extra":"{}"}],"total_entries":1}`

		client := &stubClient{response: []byte(body)}
		sch := schedulerWithClient(logger, client)

		events, err := sch.GetEventLogs(context.Background(), scheduler.AuditEventFilter{
			Tenant: tnnt, DagID: "a-job", RunID: "manual__2026-07-20T13:00:00+00:00",
		})
		require.NoError(t, err)
		require.Len(t, events, 1)

		assert.Equal(t, "a-job", events[0].DagID)
		assert.Equal(t, "manual__2026-07-20T13:00:00+00:00", events[0].RunID)
		require.NotNil(t, events[0].ExecutionDate)
		assert.True(t, events[0].HasDagContext())

		// The path must stay free of per-request values: Invoke uses it as a Prometheus label.
		assert.Equal(t, eventLogsURL, client.request.path)
		assert.NotContains(t, client.request.path, "a-job")
		assert.Contains(t, client.request.query, "dag_id=a-job")
	})

	t.Run("returns an empty slice rather than nil when airflow has nothing", func(t *testing.T) {
		sch := schedulerWithClient(logger, &stubClient{response: []byte(`{"event_logs":[],"total_entries":0}`)})

		events, err := sch.GetEventLogs(context.Background(), scheduler.AuditEventFilter{Tenant: tnnt})
		require.NoError(t, err)
		assert.NotNil(t, events)
		assert.Empty(t, events)
	})

	t.Run("surfaces an airflow failure", func(t *testing.T) {
		// A missing `can_read on Audit Logs` permission shows up here as a non-200 from Invoke.
		sch := schedulerWithClient(logger, &stubClient{err: fmt.Errorf("status code received 403")})

		_, err := sch.GetEventLogs(context.Background(), scheduler.AuditEventFilter{Tenant: tnnt})
		assert.ErrorContains(t, err, "failure while getting airflow event logs")
	})

	t.Run("surfaces a malformed airflow response", func(t *testing.T) {
		sch := schedulerWithClient(logger, &stubClient{response: []byte(`not json`)})

		_, err := sch.GetEventLogs(context.Background(), scheduler.AuditEventFilter{Tenant: tnnt})
		assert.ErrorContains(t, err, "failure while unmarshalling airflow event logs")
	})
}

func schedulerWithClient(logger log.Logger, client Client) *Scheduler {
	project, _ := tenant.NewProject("proj", map[string]string{
		tenant.ProjectSchedulerHost:  "http://airflow.example.com",
		tenant.ProjectStoragePathKey: "file://path",
	}, map[string]string{})
	secret, _ := tenant.NewPlainTextSecret(tenant.SecretSchedulerAuth, "user:pass")
	return NewScheduler(logger, nil, client, nil, &stubProjectGetter{project: project}, &stubSecretGetter{secret: secret})
}

type stubClient struct {
	response []byte
	err      error
	request  airflowRequest
}

func (c *stubClient) Invoke(_ context.Context, r airflowRequest, _ SchedulerAuth) ([]byte, error) {
	c.request = r
	return c.response, c.err
}

type stubProjectGetter struct {
	project *tenant.Project
}

func (g *stubProjectGetter) Get(context.Context, tenant.ProjectName) (*tenant.Project, error) {
	return g.project, nil
}

type stubSecretGetter struct {
	secret *tenant.PlainTextSecret
}

func (g *stubSecretGetter) Get(context.Context, tenant.ProjectName, string, string) (*tenant.PlainTextSecret, error) {
	return g.secret, nil
}

// Guard against a silent contract drift: the audit event shape Optimus unmarshals must keep
// accepting nulls in every identifier column, since Airflow leaves them unset for bulk actions.
func TestEventLogNullableFields(t *testing.T) {
	var eventLog EventLog
	require.NoError(t, json.Unmarshal([]byte(`{"event_log_id":1,"event":"action_clear","owner":"x"}`), &eventLog))

	event := eventLog.toAuditEvent()
	assert.Empty(t, event.DagID)
	assert.Empty(t, event.TaskID)
	assert.Empty(t, event.RunID)
	assert.Empty(t, event.Extra)
	assert.Nil(t, event.ExecutionDate)
}
