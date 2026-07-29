package airflow

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/cron"
)

const (
	pageLimit = 99999

	// requestTimeout bounds a single Airflow API call. Without it the shared http.Client has
	// no timeout at all, so an unresponsive webserver holds the calling goroutine forever.
	requestTimeout = 30 * time.Second
)

type airflowRequest struct {
	path   string
	query  string
	method string
	body   []byte
}

type DagRunListResponse struct {
	DagRuns      []DagRun `json:"dag_runs"`
	TotalEntries int      `json:"total_entries"`
}

type DagRun struct {
	ExecutionDate          time.Time `json:"execution_date"`
	State                  string    `json:"state"`
	ExternalTrigger        bool      `json:"external_trigger"`
	DagRunID               string    `json:"dag_run_id"`
	DagID                  string    `json:"dag_id"`
	LogicalDate            time.Time `json:"logical_date"`
	StartDate              time.Time `json:"start_date"`
	EndDate                time.Time `json:"end_date"`
	DataIntervalStart      time.Time `json:"data_interval_start"`
	DataIntervalEnd        time.Time `json:"data_interval_end"`
	LastSchedulingDecision time.Time `json:"last_scheduling_decision"`
	RunType                string    `json:"run_type"`
}

// EventLogListResponse and EventLog mirror Airflow's /eventLogs collection, which is its
// record of user actions taken through the UI or the REST API.
//
// DagID, TaskID and RunID are nullable: Airflow fills them from the originating request's
// query and form parameters only, so bulk actions taken from the /dagrun/list/ and
// /taskinstance/list/ pages, and REST calls that pass arguments in a JSON body, leave them
// unset. Extra holds whatever other request parameters were logged.
type EventLogListResponse struct {
	EventLogs    []EventLog `json:"event_logs"`
	TotalEntries int        `json:"total_entries"`
}

type EventLog struct {
	EventLogID    int64      `json:"event_log_id"`
	When          time.Time  `json:"when"`
	Event         string     `json:"event"`
	Owner         string     `json:"owner"`
	DagID         *string    `json:"dag_id"`
	TaskID        *string    `json:"task_id"`
	RunID         *string    `json:"run_id"`
	ExecutionDate *time.Time `json:"execution_date"`
	Extra         *string    `json:"extra"`
}

func (e EventLog) toAuditEvent() *scheduler.AuditEvent {
	event := &scheduler.AuditEvent{
		EventLogID:    e.EventLogID,
		When:          e.When,
		Event:         e.Event,
		Owner:         e.Owner,
		ExecutionDate: e.ExecutionDate,
	}
	if e.DagID != nil {
		event.DagID = *e.DagID
	}
	if e.TaskID != nil {
		event.TaskID = *e.TaskID
	}
	if e.RunID != nil {
		event.RunID = *e.RunID
	}
	if e.Extra != nil {
		event.Extra = *e.Extra
	}
	return event
}

type DagRunRequest struct {
	OrderBy          string   `json:"order_by"`
	PageOffset       int      `json:"page_offset"`
	PageLimit        int      `json:"page_limit"`
	DagIds           []string `json:"dag_ids"` // nolint: revive
	ExecutionDateGte string   `json:"execution_date_gte,omitempty"`
	ExecutionDateLte string   `json:"execution_date_lte,omitempty"`
}

type SchedulerAuth struct {
	host  string
	token string
}

type ClientAirflow struct {
	client *http.Client
}

var airflowAPIMetrics = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "airflow_api",
}, []string{"api_name", "status", "error"})

func NewAirflowClient() *ClientAirflow {
	return &ClientAirflow{client: &http.Client{Timeout: requestTimeout}}
}

func (ac ClientAirflow) Invoke(ctx context.Context, r airflowRequest, auth SchedulerAuth) ([]byte, error) {
	var resp []byte

	endpoint := buildEndPoint(auth.host, r.path, r.query)
	request, err := http.NewRequestWithContext(ctx, r.method, endpoint, bytes.NewBuffer(r.body))
	if err != nil {
		return resp, fmt.Errorf("failed to build http request for %s due to %w", endpoint, err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(auth.token))))

	httpResp, respErr := ac.client.Do(request)
	if respErr != nil {
		airflowAPIMetrics.WithLabelValues(r.path, "error", respErr.Error()).Inc()
		return resp, fmt.Errorf("failed to call airflow %s due to %w", endpoint, respErr)
	}
	airflowAPIMetrics.WithLabelValues(r.path, httpResp.Status, "").Inc()
	if httpResp.StatusCode != http.StatusOK {
		httpResp.Body.Close()
		return resp, fmt.Errorf("status code received %d on calling %s", httpResp.StatusCode, endpoint)
	}
	return parseResponse(httpResp)
}

func parseResponse(resp *http.Response) ([]byte, error) {
	var body []byte
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return body, errors.Wrap(EntityAirflow, "failed to read airflow response", err)
	}
	return body, nil
}

func buildEndPoint(host, path, query string) string {
	host = strings.Trim(host, "/")
	u := &url.URL{
		Scheme:   "http",
		Host:     host,
		Path:     path,
		RawQuery: query,
	}
	return u.String()
}

func getJobRuns(res DagRunListResponse, spec *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	var jobRunList []*scheduler.JobRunStatus
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.InternalError(EntityAirflow, "total number of entries exceed page limit", nil)
	}
	for _, dag := range res.DagRuns {
		scheduledAt := spec.Next(dag.ExecutionDate)
		jobRunStatus, _ := scheduler.JobRunStatusFrom(scheduledAt, dag.State)
		// use multi error to collect errors and proceed
		jobRunList = append(jobRunList, &jobRunStatus)
	}
	return jobRunList, nil
}

func getJobRunsForReplay(res *DagRunListResponse, spec *cron.ScheduleSpec) ([]*scheduler.JobRunStatus, error) {
	var jobRunList []*scheduler.JobRunStatus
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.InternalError(EntityAirflow, "total number of entries exceed page limit", nil)
	}
	for _, dag := range res.DagRuns {
		scheduledAt := spec.Next(dag.ExecutionDate)
		if spec.Prev(scheduledAt) != dag.ExecutionDate {
			// previous execution date created with some other cron interval
			continue
		}
		jobRunStatus, _ := scheduler.JobRunStatusFrom(scheduledAt, dag.State)
		// use multi error to collect errors and proceed
		jobRunList = append(jobRunList, &jobRunStatus)
	}
	return jobRunList, nil
}

func getJobRunsWithDetails(res DagRunListResponse, spec *cron.ScheduleSpec) ([]*scheduler.JobRunWithDetails, error) {
	var jobRunList []*scheduler.JobRunWithDetails
	if res.TotalEntries > pageLimit {
		return jobRunList, errors.InternalError(EntityAirflow, "total number of entries exceed page limit", nil)
	}
	for _, dag := range res.DagRuns {
		scheduledAt := spec.Next(dag.ExecutionDate)
		jobRunStatus, _ := scheduler.StateFromString(dag.State)
		jobRunList = append(jobRunList, &scheduler.JobRunWithDetails{
			ScheduledAt:     scheduledAt,
			State:           jobRunStatus,
			RunType:         dag.RunType,
			ExternalTrigger: dag.ExternalTrigger,
			DagRunID:        dag.DagRunID,
			DagID:           dag.DagID,
		})
	}
	return jobRunList, nil
}

func startChildSpan(ctx context.Context, name string) (context.Context, trace.Span) {
	tracer := otel.Tracer("scheduler/airflow")

	return tracer.Start(ctx, name)
}
