package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

func TestFromStringToEventType(t *testing.T) {
	t.Run("FromStringToEventType", func(t *testing.T) {
		expectationMap := map[string]scheduler.JobEventType{
			"TYPE_SLA_MISS":    scheduler.SLAMissEvent,
			"TYPE_FAILURE":     scheduler.JobFailureEvent,
			"TYPE_JOB_SUCCESS": scheduler.JobSuccessEvent,

			"TYPE_TASK_START":   scheduler.TaskStartEvent,
			"TYPE_TASK_RETRY":   scheduler.TaskRetryEvent,
			"TYPE_TASK_FAIL":    scheduler.TaskFailEvent,
			"TYPE_TASK_SUCCESS": scheduler.TaskSuccessEvent,

			"TYPE_SENSOR_START":   scheduler.SensorStartEvent,
			"TYPE_SENSOR_RETRY":   scheduler.SensorRetryEvent,
			"TYPE_SENSOR_FAIL":    scheduler.SensorFailEvent,
			"TYPE_SENSOR_SUCCESS": scheduler.SensorSuccessEvent,

			"TYPE_HOOK_START":   scheduler.HookStartEvent,
			"TYPE_HOOK_RETRY":   scheduler.HookRetryEvent,
			"TYPE_HOOK_FAIL":    scheduler.HookFailEvent,
			"TYPE_HOOK_SUCCESS": scheduler.HookSuccessEvent,

			"UNREGISTERED_EVENT": "",
		}

		for input, expectation := range expectationMap {
			output, err := scheduler.FromStringToEventType(input)

			if input != "UNREGISTERED_EVENT" {
				assert.Nil(t, err)
			} else {
				assert.NotNil(t, err)
			}

			assert.Equal(t, expectation, output)
		}
	})
	t.Run("EventFrom", func(t *testing.T) {
		t.Run("Should return error if scheduled_at is incorrect format", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022--01-02T15:04:05Z",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'scheduled_at' is not in appropriate format")
			assert.Nil(t, eventObj)
		})
		t.Run("Should return error if scheduled_at is not provided in event payload", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":    "someValue",
				"event_time": 16000631600.0,
				"task_id":    "some_txbq",
				"status":     "running",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'scheduled_at'(string) is missing in event payload")
			assert.Nil(t, eventObj)
		})
		t.Run("Should return error if task_id is not provided in event payload", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'task_id'(string) is missing in event payload")
			assert.Nil(t, eventObj)
		})
		t.Run("Should return error if event is notvalid number", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   "16000631600.0",
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: property 'event_time'(number) is missing in event payload")
			assert.Nil(t, eventObj)
		})
		t.Run("Should parse events of type slaMiss", func(t *testing.T) {
			sla := []map[string]string{
				{
					"dag_id":       "sample_select",
					"scheduled_at": "2006-01-02T15:04:05Z",
				},
				{
					"dag_id":       "sample_select1",
					"scheduled_at": "2006-01-02T15:04:05Z",
				},
			}
			eventValues := map[string]any{
				"event_time": "16000631600.0",
				"slas":       sla,
			}
			jobName := scheduler.JobName("")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_SLA_MISS"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.Nil(t, err)

			scheduledAt, _ := time.Parse(scheduler.ISODateFormat, "2006-01-02T15:04:05Z")

			assert.Equal(t, eventObj, &scheduler.Event{
				Tenant: tnnt,
				Type:   scheduler.SLAMissEvent,
				Values: eventValues,
				SLAObjectList: []*scheduler.SLAObject{
					{
						JobName:        "sample_select",
						JobScheduledAt: scheduledAt,
					},
					{
						JobName:        "sample_select1",
						JobScheduledAt: scheduledAt,
					},
				},
			})
		})
		t.Run("Should return error if event is unregistered type", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			assert.Nil(t, err)

			eventTypeName := "TYPE_TASK_RETRY_UNREGISTERED"
			eventObj, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)

			assert.NotNil(t, err)
			assert.EqualError(t, err, "invalid argument for entity event: unknown event task_retry_unregistered")
			assert.Nil(t, eventObj)
		})
		t.Run("Should Successfully parse an event", func(t *testing.T) {
			eventValues := map[string]any{
				"someKey":      "someValue",
				"event_time":   16000631600.0,
				"task_id":      "some_txbq",
				"status":       "running",
				"scheduled_at": "2022-01-02T15:04:05Z",
				"event_context": map[string]any{
					"task_instance": map[string]any{
						"max_tries":             1,
						"task_id":               "mc2mc",
						"task_instance_key_str": "campaign__mc2mc__20250926",
						"attempt":               2,
						"log_url":               "http://localhost:8080/dags/campaign/grid?dag_run_id=scheduled__2025-09-26T03%3A40%3A00%2B00%3A00&task_id=mc2mc&map_index=-1&tab=logs",
						"start_date":            "2025-09-26T03:46:39Z",
					},
					"dag_run": map[string]any{
						"dag_id":         "campaign",
						"scheduled_at":   "2025-09-26T03:50:00Z",
						"execution_date": "2025-09-26T03:40:00Z",
						"run_id":         "scheduled__2025-09-26T03:40:00+00:00",
						"start_date":     "2025-09-26T03:45:00Z",
					},
					"task":          map[string]any{"downstream_task_ids": []string{"hook_predator"}},
					"operator_type": "TASK",
					"event_type":    "operator_retry",
				},
				"event_type": "TYPE_TASK_RETRY",
			}
			jobName := scheduler.JobName("some_job")
			tnnt, err := tenant.NewTenant("someProject", "someNamespace")
			eventTypeName := "TYPE_TASK_RETRY"
			assert.Nil(t, err)

			startTime, _ := time.Parse(time.RFC3339, "2025-09-26T03:46:39Z")
			jobScheduledAt, _ := time.Parse(time.RFC3339, "2025-09-26T03:50:00Z")
			ExecutionDate, _ := time.Parse(time.RFC3339, "2025-09-26T03:40:00Z")
			jobstartDate, _ := time.Parse(time.RFC3339, "2025-09-26T03:45:00Z")
			outputObj := scheduler.Event{
				JobName:        jobName,
				Tenant:         tnnt,
				Type:           scheduler.TaskRetryEvent,
				EventTime:      time.Date(2477, time.January, 14, 11, 53, 20, 0, time.UTC),
				OperatorName:   "some_txbq",
				Status:         scheduler.StateRunning,
				JobScheduledAt: time.Date(2022, time.January, 2, 15, 0o4, 0o5, 0, time.UTC),
				Values:         eventValues,
				EventContext: &scheduler.EventContext{
					Type:         scheduler.OperatorRetryEvent,
					OperatorType: scheduler.OperatorTask,
					OperatorRunInstance: scheduler.OperatorRunInstance{
						MaxTries:     1,
						OperatorName: "mc2mc",
						StartTime:    startTime,
						OperatorKey:  "campaign__mc2mc__20250926",
						TryNumber:    2,
						EndTime:      nil,
						LogURL:       "http://localhost:8080/dags/campaign/grid?dag_run_id=scheduled__2025-09-26T03%3A40%3A00%2B00%3A00&task_id=mc2mc&map_index=-1&tab=logs",
					},
					DagRun: scheduler.DagRun{
						RunID:         "scheduled__2025-09-26T03:40:00+00:00",
						JobName:       "campaign",
						ScheduledAt:   jobScheduledAt,
						ExecutionDate: ExecutionDate,
						StartTime:     jobstartDate,
						EndTime:       nil,
					},
					Task: scheduler.OperatorObj{
						DownstreamTaskIDs: []string{"hook_predator"},
					},
					EventReason: nil,
				},
			}
			output, err := scheduler.EventFrom(eventTypeName, eventValues, jobName, tnnt)
			assert.Nil(t, err)
			assert.Equal(t, outputObj.JobScheduledAt, output.JobScheduledAt)
			assert.Equal(t, &outputObj, output)
		})
	})
	t.Run("IsOfType EventCategory", func(t *testing.T) {
		positiveExpectationMap := map[scheduler.JobEventType]scheduler.EventCategory{
			scheduler.JobFailureEvent: scheduler.EventCategoryJobFailure,
			scheduler.SLAMissEvent:    scheduler.EventCategorySLAMiss,
		}
		for eventType, category := range positiveExpectationMap {
			assert.True(t, eventType.IsOfType(category))
		}
		negativeExpectationMap := map[scheduler.JobEventType]scheduler.EventCategory{
			scheduler.SLAMissEvent:       scheduler.EventCategoryJobFailure,
			scheduler.SensorRetryEvent:   scheduler.EventCategoryJobFailure,
			scheduler.SensorSuccessEvent: scheduler.EventCategorySLAMiss,
		}
		for eventType, category := range negativeExpectationMap {
			assert.False(t, eventType.IsOfType(category))
		}
	})
}
