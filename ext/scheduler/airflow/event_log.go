package airflow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

const (
	eventLogsURL = "api/v1/eventLogs"

	// eventLogPageSize is Airflow's own [api] maximum_page_limit default; requesting more
	// gets silently clamped server-side (Airflow returns 200, not an error, per
	// airflow/api_connexion/parameters.py check_limit), so there is no benefit to asking
	// for more and it would just make the clamp implicit instead of explicit here.
	eventLogPageSize = 100

	eventTaskSuccess   = "success"
	eventTaskFailed    = "failed"
	eventDagRunSuccess = "dagrun_success"
	eventDagRunFailed  = "dagrun_failed"
)

// manualOverrideEventNames is pushed down to Airflow via included_events so the server does
// as much of the filtering as it can; extra/confirmed/run_id filtering still has to happen
// client-side (see the RFC's "Volume" section for why: Airflow does not expose an
// extra-is-not-null filter, so this still returns worker-written rows for these same event
// names and callers must not assume every returned row is a manual override).
var manualOverrideEventNames = []string{eventTaskSuccess, eventTaskFailed, eventDagRunSuccess, eventDagRunFailed}

type eventLogListResponse struct {
	EventLogs    []eventLogEntry `json:"event_logs"`
	TotalEntries int             `json:"total_entries"`
}

type eventLogEntry struct {
	EventLogID int64   `json:"event_log_id"`
	DagID      *string `json:"dag_id"`
	TaskID     *string `json:"task_id"`
	RunID      *string `json:"run_id"`
	Event      string  `json:"event"`
	Owner      string  `json:"owner"`
	// Extra is a JSON-encoded string, not a nested object -- Airflow's eventLogs schema
	// returns it that way (confirmed against a live 2.9.3 response).
	Extra *string `json:"extra"`
	When  string  `json:"when"`
}

// GetEventLogs fetches every manual state override recorded in this project's Airflow audit
// log within [after, before), paginating until Airflow's own total_entries is exhausted. Both
// bounds are strict on the Airflow side (dttm < before, dttm > after); callers doing windowed
// polling are responsible for any overlap/de-duplication across adjacent windows -- this
// method just reports what a single [after, before) query returns.
//
// See docs/docs/rfcs/20260727_manual_state_override_reconciliation.md for why the four bare
// event names plus a populated `extra` (and, for dagrun_* events, extra["confirmed"]=="true")
// are what identify a manual override, as opposed to an ordinary worker-driven transition.
func (s *Scheduler) GetEventLogs(ctx context.Context, projectName tenant.ProjectName, after, before time.Time) ([]scheduler.ManualOverrideEvent, error) {
	spanCtx, span := startChildSpan(ctx, "GetEventLogs")
	defer span.End()

	schdAuth, err := s.getSchedulerAuth(spanCtx, projectName)
	if err != nil {
		return nil, err
	}

	var events []scheduler.ManualOverrideEvent
	offset := 0
	for {
		page, err := s.fetchEventLogPage(spanCtx, schdAuth, after, before, offset)
		if err != nil {
			return nil, errors.Wrap(EntityAirflow, "failure while fetching airflow event logs", err)
		}

		for i := range page.EventLogs {
			event, ok, err := toManualOverrideEvent(page.EventLogs[i])
			if err != nil {
				// one malformed row should not block the rest of the window from being
				// reconciled -- log and move on rather than aborting the whole fetch.
				s.l.Warn("skipping unparseable airflow event log row", "event_log_id", page.EventLogs[i].EventLogID, "error", err)
				continue
			}
			if ok {
				events = append(events, event)
			}
		}

		offset += len(page.EventLogs)
		if len(page.EventLogs) == 0 || offset >= page.TotalEntries {
			break
		}
	}
	return events, nil
}

func (s *Scheduler) fetchEventLogPage(ctx context.Context, schdAuth SchedulerAuth, after, before time.Time, offset int) (*eventLogListResponse, error) {
	params := url.Values{}
	params.Add("after", after.UTC().Format(time.RFC3339))
	params.Add("before", before.UTC().Format(time.RFC3339))
	params.Add("included_events", strings.Join(manualOverrideEventNames, ","))
	params.Add("order_by", "event_log_id")
	params.Add("limit", strconv.Itoa(eventLogPageSize))
	params.Add("offset", strconv.Itoa(offset))

	req := airflowRequest{
		path:   eventLogsURL,
		method: http.MethodGet,
		query:  params.Encode(),
	}

	resp, err := s.client.Invoke(ctx, req, schdAuth)
	if err != nil {
		return nil, err
	}
	return unmarshalAs[eventLogListResponse](resp)
}

// toManualOverrideEvent maps one raw eventLogs row onto scheduler.ManualOverrideEvent.
// ok is false when the row should be skipped without error: a worker-written row (extra is
// nil -- for these four event names extra is only ever populated by Airflow's
// action_logging), an unconfirmed dagrun preview that changed nothing, or a run_id outside
// v1's scope.
func toManualOverrideEvent(e eventLogEntry) (event scheduler.ManualOverrideEvent, ok bool, err error) {
	if e.Extra == nil {
		return scheduler.ManualOverrideEvent{}, false, nil
	}

	var extraFields map[string]string
	if err := json.Unmarshal([]byte(*e.Extra), &extraFields); err != nil {
		return scheduler.ManualOverrideEvent{}, false, fmt.Errorf("event_log_id %d: unparseable extra: %w", e.EventLogID, err)
	}

	isDagRunEvent := e.Event == eventDagRunSuccess || e.Event == eventDagRunFailed
	if isDagRunEvent {
		// _mark_dagrun_state_as_success/failed pass commit=confirmed, so a confirmed=false
		// row is a preview that changed nothing in Airflow -- logged, but nothing to
		// reconcile. Task-level success/failed ignore `confirmed` entirely in 2.9.3 and
		// always apply, so this check must not be applied to those two events.
		if extraFields["confirmed"] != "true" {
			return scheduler.ManualOverrideEvent{}, false, nil
		}
	}

	if e.RunID == nil {
		return scheduler.ManualOverrideEvent{}, false, nil
	}

	when, err := time.Parse(time.RFC3339, e.When)
	if err != nil {
		return scheduler.ManualOverrideEvent{}, false, fmt.Errorf("event_log_id %d: unparseable when %q: %w", e.EventLogID, e.When, err)
	}

	var dagID, taskID string
	if e.DagID != nil {
		dagID = *e.DagID
	}
	if e.TaskID != nil {
		taskID = *e.TaskID
	}

	manualOverride := scheduler.ManualOverrideEvent{
		LogID:  e.EventLogID,
		Event:  e.Event,
		DagID:  dagID,
		TaskID: taskID,
		RunID:  *e.RunID,
		Owner:  e.Owner,
		When:   when,
	}

	// Allow-list, not deny-list: an unrecognised run_id prefix is skipped rather than
	// guessed at, so an Airflow run-id convention we haven't seen gets noticed (via the
	// service layer's skip metric) instead of mis-parsed.
	if !manualOverride.HasRecognisedRunIDPrefix() {
		return scheduler.ManualOverrideEvent{}, false, nil
	}

	return manualOverride, true, nil
}
