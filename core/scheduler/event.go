package scheduler

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"
)

type (
	EventName     string
	EventStatus   string
	JobEventType  string
	EventCategory string
)

type OperatorsSLA struct {
	ID           uuid.UUID
	JobName      JobName
	ProjectName  tenant.ProjectName
	OperatorName string
	RunID        string
	OperatorType OperatorType
	SLATime      time.Time

	AlertTag string

	ScheduledAt       time.Time
	OperatorStartTime time.Time

	WorkerSignature string
	WorkerLockUntil time.Time
}

func (o OperatorsSLA) String() string {
	return fmt.Sprintf("%s:%s:%s, SLATime: %s, OperatorStartTime: %s", o.JobName, o.OperatorName, o.RunID, o.SLATime, o.OperatorStartTime)
}

const (
	EntityEvent = "event"

	ISODateFormat = "2006-01-02T15:04:05Z"

	EventCategorySLAMiss    EventCategory = "sla_miss"
	EventCategoryJobFailure EventCategory = "failure"
	EventCategoryJobSuccess EventCategory = "job_success"
	EventCategoryReplay     EventCategory = "replay_lifecycle"

	SLAMissEvent    JobEventType = "sla_miss"
	JobFailureEvent JobEventType = "failure"
	JobSuccessEvent JobEventType = "job_success"
	ReplayEvent     JobEventType = "replay_lifecycle"

	TaskStartEvent   JobEventType = "task_start"
	TaskRetryEvent   JobEventType = "task_retry"
	TaskFailEvent    JobEventType = "task_fail"
	TaskSuccessEvent JobEventType = "task_success"

	HookStartEvent   JobEventType = "hook_start"
	HookRetryEvent   JobEventType = "hook_retry"
	HookFailEvent    JobEventType = "hook_fail"
	HookSuccessEvent JobEventType = "hook_success"

	SensorStartEvent   JobEventType = "sensor_start"
	SensorRetryEvent   JobEventType = "sensor_retry"
	SensorFailEvent    JobEventType = "sensor_fail"
	SensorSuccessEvent JobEventType = "sensor_success"

	OperatorStartEvent   JobEventType = "operator_start"
	OperatorRetryEvent   JobEventType = "operator_retry"
	OperatorFailEvent    JobEventType = "operator_fail"
	OperatorSuccessEvent JobEventType = "operator_success"

	StatusFiring   EventStatus = "firing"
	StatusResolved EventStatus = "resolved"
)

func FromStringToEventType(name string) (JobEventType, error) {
	name = strings.TrimPrefix(strings.ToLower(name), strings.ToLower("TYPE_"))
	switch name {
	case string(JobFailureEvent):
		return JobFailureEvent, nil
	case string(JobSuccessEvent):
		return JobSuccessEvent, nil
	case string(SLAMissEvent):
		return SLAMissEvent, nil
	case string(TaskStartEvent):
		return TaskStartEvent, nil
	case string(TaskRetryEvent):
		return TaskRetryEvent, nil
	case string(TaskFailEvent):
		return TaskFailEvent, nil
	case string(TaskSuccessEvent):
		return TaskSuccessEvent, nil
	case string(HookStartEvent):
		return HookStartEvent, nil
	case string(HookRetryEvent):
		return HookRetryEvent, nil
	case string(HookFailEvent):
		return HookFailEvent, nil
	case string(HookSuccessEvent):
		return HookSuccessEvent, nil
	case string(SensorStartEvent):
		return SensorStartEvent, nil
	case string(SensorRetryEvent):
		return SensorRetryEvent, nil
	case string(SensorFailEvent):
		return SensorFailEvent, nil
	case string(SensorSuccessEvent):
		return SensorSuccessEvent, nil
	default:
		return "", errors.InvalidArgument(EntityEvent, "unknown event "+name)
	}
}

type SLAObject struct {
	JobName        JobName
	JobScheduledAt time.Time
}

func (s *SLAObject) String() string {
	return fmt.Sprintf("(job: %s,scheduledAt: %s)", s.JobName, s.JobScheduledAt.Format(time.RFC3339))
}

type OperatorRunInstance struct {
	MaxTries     int        `json:"max_tries"`
	OperatorName string     `json:"task_id"`
	StartTime    time.Time  `json:"start_date"`
	OperatorKey  string     `json:"task_instance_key_str"`
	TryNumber    int        `json:"attempt"`
	State        string     `json:"status"`
	EndTime      *time.Time `json:"end_date,omitempty"`
	LogURL       string     `json:"log_url"`
}

func (o *OperatorRunInstance) IsTerminated() bool {
	if strings.EqualFold(o.State, StateFailed.String()) || strings.EqualFold(o.State, StateSuccess.String()) {
		return true
	}
	return false
}

type DagRun struct {
	RunID         string     `json:"run_id"`
	JobName       string     `json:"dag_id"`
	ScheduledAt   time.Time  `json:"scheduled_at"`
	ExecutionDate time.Time  `json:"execution_date"`
	StartTime     time.Time  `json:"start_date"`
	EndTime       *time.Time `json:"end_date,omitempty"`
}

type OperatorObj struct {
	DownstreamTaskIDs []string `json:"downstream_task_ids"`
}

type EventContext struct {
	Tenant              tenant.Tenant
	Type                JobEventType        `json:"event_type"`
	OperatorType        OperatorType        `json:"operator_type"`
	OperatorRunInstance OperatorRunInstance `json:"task_instance"`
	DagRun              DagRun              `json:"dag_run"`
	Task                OperatorObj         `json:"task"`
	EventReason         *string             `json:"event_reason,omitempty"`
}

type Event struct {
	JobName        JobName
	URN            string
	Tenant         tenant.Tenant
	Type           JobEventType
	EventTime      time.Time
	OperatorName   string
	Status         State
	JobScheduledAt time.Time
	Values         map[string]any
	SLAObjectList  []*SLAObject

	EventContext *EventContext
}

func (e Event) String() string {
	data, err := json.Marshal(e)
	if err != nil {
		return fmt.Sprintf("error marshalling Event: %s, job: %s, operator: %s, err: %s", e.Type, e.JobName, e.OperatorName, err.Error())
	}
	return string(data)
}

func (e EventCategory) String() string {
	return string(e)
}

func (event JobEventType) IsOfType(category EventCategory) bool {
	switch category {
	case EventCategoryJobFailure:
		if event == JobFailureEvent {
			return true
		}
	case EventCategoryJobSuccess:
		if event == JobSuccessEvent {
			return true
		}
	case EventCategorySLAMiss:
		if event == SLAMissEvent {
			return true
		}
	case EventCategoryReplay:
		if event == ReplayEvent {
			return true
		}
	}
	return false
}

func (event JobEventType) String() string {
	return string(event)
}

func EventContextFrom(rawEventContext any) (*EventContext, error) {
	sc := EventContext{}
	bytesArr, err := json.Marshal(rawEventContext)
	if err != nil {
		return nil, errors.InvalidArgument(EntityEvent, "unable to marshal event")
	}
	err = json.Unmarshal(bytesArr, &sc)
	if err != nil {
		return nil, errors.InvalidArgument(EntityEvent, "unable to unmarshal event, err:"+err.Error())
	}
	sc.OperatorType, err = NewOperatorType(strings.ToLower(sc.OperatorType.String()))
	if err != nil {
		return nil, err
	}
	return &sc, nil
}

func EventFrom(eventTypeName string, eventValues map[string]any, jobName JobName, tenent tenant.Tenant) (*Event, error) {
	eventType, err := FromStringToEventType(eventTypeName)
	if err != nil {
		return nil, err
	}
	eventObj := Event{
		JobName: jobName,
		Tenant:  tenent,
		Type:    eventType,
		Values:  eventValues,
	}

	if eventType.IsOfType(EventCategorySLAMiss) {
		type slaInput struct {
			Slas []struct {
				DagID       string `mapstructure:"dag_id"`
				ScheduledAt string `mapstructure:"scheduled_at"`
			} `mapstructure:"slas"`
		}
		var slaInputPayload slaInput
		err = mapstructure.Decode(eventValues, &slaInputPayload)
		if err != nil {
			return nil, errors.InvalidArgument(EntityEvent, "bad sla payload")
		}
		var slaObjectList []*SLAObject
		for _, slaObject := range slaInputPayload.Slas {
			schedulerJobName, err := JobNameFrom(slaObject.DagID)
			if err != nil {
				return nil, errors.InvalidArgument(EntityEvent, "empty job name")
			}
			scheduledAt, err := time.Parse(ISODateFormat, slaObject.ScheduledAt)
			if err != nil {
				return nil, errors.InvalidArgument(EntityEvent, "property 'scheduled_at' in slas list is not in appropriate format")
			}
			slaObjectList = append(slaObjectList, &SLAObject{
				JobName:        schedulerJobName,
				JobScheduledAt: scheduledAt,
			})
		}
		if len(slaObjectList) == 0 {
			return nil, errors.InvalidArgument(EntityEvent, "could not parse sla list or received an empty sla list nothing to process")
		}
		eventObj.SLAObjectList = slaObjectList
	} else {
		statusString := utils.ConfigAs[string](eventValues, "status")
		status, err := StateFromString(statusString)
		if err != nil {
			return nil, err
		}
		eventObj.Status = status

		eventTimeFloat := utils.ConfigAs[float64](eventValues, "event_time")
		if eventTimeFloat == float64(0) {
			return nil, errors.InvalidArgument(EntityEvent, "property 'event_time'(number) is missing in event payload")
		}
		eventObj.EventTime = time.Unix(int64(eventTimeFloat), 0).UTC()

		operatorName := utils.ConfigAs[string](eventValues, "task_id")
		if operatorName == "" {
			return nil, errors.InvalidArgument(EntityEvent, "property 'task_id'(string) is missing in event payload")
		}
		eventObj.OperatorName = operatorName

		scheduledAtString := utils.ConfigAs[string](eventValues, "scheduled_at")
		if scheduledAtString == "" {
			return nil, errors.InvalidArgument(EntityEvent, "property 'scheduled_at'(string) is missing in event payload")
		}
		scheduledAtTimeStamp, err := time.Parse(ISODateFormat, scheduledAtString)
		if err != nil {
			return nil, errors.InvalidArgument(EntityEvent, "property 'scheduled_at' is not in appropriate format")
		}
		eventObj.JobScheduledAt = scheduledAtTimeStamp

		switch eventType {
		case TaskFailEvent, TaskStartEvent, TaskRetryEvent, TaskSuccessEvent,
			HookFailEvent, HookStartEvent, HookRetryEvent, HookSuccessEvent,
			SensorFailEvent, SensorStartEvent, SensorRetryEvent, SensorSuccessEvent:

			eventObj.EventContext, err = EventContextFrom(eventValues["event_context"])
			if err != nil {
				return nil, err
			}
			eventObj.EventContext.Tenant = eventObj.Tenant
		}
	}

	return &eventObj, nil
}
