package event

import (
	"github.com/gogo/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/internal/utils"
	pbCore "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
	pbInt "github.com/goto/optimus/protos/gotocompany/optimus/integration/v1beta1"
)

type JobCreated struct {
	Event

	job *job.Job
}

func (j JobCreated) Bytes() ([]byte, error) {
	return jobEventToBytes(j.Event, j.job, pbInt.OptimusChangeEvent_JOB_CREATE)
}

type JobUpdated struct {
	Event

	job *job.Job
}

func (j JobUpdated) Bytes() ([]byte, error) {
	return jobEventToBytes(j.Event, j.job, pbInt.OptimusChangeEvent_JOB_UPDATE)
}

type JobDeleted struct {
	Event

	job *job.Job
}

func (j JobDeleted) Bytes() ([]byte, error) {
	return jobEventToBytes(j.Event, j.job, pbInt.OptimusChangeEvent_JOB_DELETE)
}

func jobEventToBytes(event Event, job *job.Job, eventType pbInt.OptimusChangeEvent_EventType) ([]byte, error) {
	jobPb := &pbCore.JobSpecification{
		Version:          int32(job.Spec().Version()),
		Name:             job.Spec().Name().String(),
		Owner:            job.Spec().Owner(),
		StartDate:        job.Spec().Schedule().StartDate().String(),
		EndDate:          job.Spec().Schedule().EndDate().String(),
		Interval:         job.Spec().Schedule().Interval(),
		DependsOnPast:    job.Spec().Schedule().DependsOnPast(),
		CatchUp:          job.Spec().Schedule().CatchUp(),
		TaskName:         job.Spec().Task().Name().String(),
		Config:           fromConfig(job.Spec().Task().Config()),
		WindowSize:       job.Spec().Window().GetSize(),
		WindowOffset:     job.Spec().Window().GetOffset(),
		WindowTruncateTo: job.Spec().Window().GetTruncateTo(),
		Dependencies:     fromSpecUpstreams(job.Spec().UpstreamSpec()),
		Assets:           fromAsset(job.Spec().Asset()),
		Hooks:            fromHooks(job.Spec().Hooks()),
		Description:      job.Spec().Description(),
		Labels:           job.Spec().Labels(),
		Behavior:         fromRetryAndAlerts(job.Spec().Schedule().Retry(), job.Spec().AlertSpecs()),
		Metadata:         fromMetadata(job.Spec().Metadata()),
		Destination:      job.Destination().String(),
		Sources:          fromResourceURNs(job.Sources()),
	}

	occurredAt := timestamppb.New(event.OccurredAt)
	optEvent := &pbInt.OptimusChangeEvent{
		EventId:       event.ID.String(),
		OccurredAt:    occurredAt,
		ProjectName:   string(job.Tenant().ProjectName()),
		NamespaceName: job.Tenant().NamespaceName().String(),
		EventType:     eventType,
		Payload: &pbInt.OptimusChangeEvent_JobChange{
			JobChange: &pbInt.JobChangePayload{
				JobName: jobPb.GetName(),
				JobSpec: jobPb,
			},
		},
	}
	return proto.Marshal(optEvent)
}

func fromResourceURNs(resourceURNs []job.ResourceURN) []string {
	var resources []string
	for _, resourceURN := range resourceURNs {
		resources = append(resources, resourceURN.String())
	}
	return resources
}

func fromMetadata(metadata *job.Metadata) *pbCore.JobMetadata {
	if metadata == nil {
		return nil
	}

	metadataResourceProto := &pbCore.JobSpecMetadataResource{}
	if metadata.Resource() != nil {
		if metadata.Resource().Request() != nil {
			metadataResourceProto.Request = &pbCore.JobSpecMetadataResourceConfig{
				Cpu:    metadata.Resource().Request().CPU(),
				Memory: metadata.Resource().Request().Memory(),
			}
		}
		if metadata.Resource().Limit() != nil {
			metadataResourceProto.Limit = &pbCore.JobSpecMetadataResourceConfig{
				Cpu:    metadata.Resource().Limit().CPU(),
				Memory: metadata.Resource().Limit().Memory(),
			}
		}
	}

	metadataSchedulerProto := &pbCore.JobSpecMetadataAirflow{}
	if metadata.Scheduler() != nil {
		scheduler := metadata.Scheduler()
		if _, ok := scheduler["pool"]; ok {
			metadataSchedulerProto.Pool = metadata.Scheduler()["pool"]
		}
		if _, ok := scheduler["queue"]; ok {
			metadataSchedulerProto.Queue = metadata.Scheduler()["queue"]
		}
	}
	return &pbCore.JobMetadata{
		Resource: metadataResourceProto,
		Airflow:  metadataSchedulerProto,
	}
}

func fromAlerts(jobAlerts []*job.AlertSpec) []*pbCore.JobSpecification_Behavior_Notifiers {
	var notifiers []*pbCore.JobSpecification_Behavior_Notifiers
	for _, alert := range jobAlerts {
		notifiers = append(notifiers, &pbCore.JobSpecification_Behavior_Notifiers{
			On:       pbCore.JobEvent_Type(pbCore.JobEvent_Type_value[utils.ToEnumProto(alert.On(), "type")]),
			Channels: alert.Channels(),
			Config:   alert.Config(),
		})
	}
	return notifiers
}

func fromRetryAndAlerts(jobRetry *job.Retry, alerts []*job.AlertSpec) *pbCore.JobSpecification_Behavior {
	retryProto := fromRetry(jobRetry)
	notifierProto := fromAlerts(alerts)
	if retryProto == nil && len(notifierProto) == 0 {
		return nil
	}
	return &pbCore.JobSpecification_Behavior{
		Retry:  retryProto,
		Notify: notifierProto,
	}
}

func fromRetry(jobRetry *job.Retry) *pbCore.JobSpecification_Behavior_Retry {
	if jobRetry == nil {
		return nil
	}
	return &pbCore.JobSpecification_Behavior_Retry{
		Count:              int32(jobRetry.Count()),
		Delay:              &durationpb.Duration{Nanos: jobRetry.Delay()},
		ExponentialBackoff: jobRetry.ExponentialBackoff(),
	}
}

func fromHooks(hooks []*job.Hook) []*pbCore.JobSpecHook {
	var hooksProto []*pbCore.JobSpecHook
	for _, hook := range hooks {
		hooksProto = append(hooksProto, &pbCore.JobSpecHook{
			Name:   hook.Name(),
			Config: fromConfig(hook.Config()),
		})
	}
	return hooksProto
}

func fromAsset(jobAsset job.Asset) map[string]string {
	var assets map[string]string
	if jobAsset != nil {
		assets = jobAsset
	}
	return assets
}

func fromSpecUpstreams(upstreams *job.UpstreamSpec) []*pbCore.JobDependency {
	if upstreams == nil {
		return nil
	}
	var dependencies []*pbCore.JobDependency
	for _, upstreamName := range upstreams.UpstreamNames() {
		dependencies = append(dependencies, &pbCore.JobDependency{Name: upstreamName.String()}) // TODO: upstream type?
	}
	for _, httpUpstream := range upstreams.HTTPUpstreams() {
		dependencies = append(dependencies, &pbCore.JobDependency{
			HttpDependency: &pbCore.HttpDependency{
				Name:    httpUpstream.Name(),
				Url:     httpUpstream.URL(),
				Headers: httpUpstream.Headers(),
				Params:  httpUpstream.Params(),
			},
		})
	}
	return dependencies
}

func fromConfig(jobConfig job.Config) []*pbCore.JobConfigItem {
	configs := []*pbCore.JobConfigItem{}
	for configName, configValue := range jobConfig {
		configs = append(configs, &pbCore.JobConfigItem{Name: configName, Value: configValue})
	}
	return configs
}
