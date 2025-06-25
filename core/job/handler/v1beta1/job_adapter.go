package v1beta1

import (
	"fmt"

	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/lib/labels"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
	"github.com/goto/optimus/internal/utils"
	pb "github.com/goto/optimus/protos/gotocompany/optimus/core/v1beta1"
)

func ToJobProto(jobEntity *job.Job) *pb.JobSpecification {
	spec := jobEntity.Spec()

	j := &pb.JobSpecification{
		Version:        int32(spec.Version()),
		Name:           spec.Name().String(),
		Owner:          spec.Owner(),
		StartDate:      spec.Schedule().StartDate().String(),
		EndDate:        spec.Schedule().EndDate().String(),
		SchedulerState: jobEntity.State().String(),
		Interval:       spec.Schedule().Interval(),
		DependsOnPast:  spec.Schedule().DependsOnPast(),
		CatchUp:        jobEntity.Spec().Schedule().CatchUp(),
		TaskName:       spec.Task().Name().String(),
		Config:         fromConfig(spec.Task().Config()),
		Task: &pb.JobSpecTask{
			Name:    spec.Task().Name().String(),
			Version: spec.Task().Version(),
			Config:  fromConfig(spec.Task().Config()),
		},
		Dependencies: fromSpecUpstreams(spec.UpstreamSpec()),
		Assets:       fromAsset(spec.Asset()),
		Hooks:        fromHooks(spec.Hooks()),
		Description:  spec.Description(),
		Labels:       spec.Labels(),
		Behavior:     fromRetryAndAlerts(spec.Schedule().Retry(), spec.AlertSpecs()),
		Metadata:     fromMetadata(spec.Metadata()),
		Destination:  jobEntity.Destination().String(),
		Sources:      fromResourceURNs(jobEntity.Sources()),
	}

	if spec.Version() == window.NewWindowVersion {
		j.Window = &pb.JobSpecification_Window{
			Preset:     spec.WindowConfig().Preset,
			Size:       spec.WindowConfig().GetSimpleConfig().Size,
			ShiftBy:    spec.WindowConfig().GetSimpleConfig().ShiftBy,
			Location:   spec.WindowConfig().GetSimpleConfig().Location,
			TruncateTo: spec.WindowConfig().GetSimpleConfig().TruncateTo,
		}
	} else {
		j.WindowPreset = spec.WindowConfig().Preset
		j.WindowSize = spec.WindowConfig().GetSize()
		j.WindowOffset = spec.WindowConfig().GetOffset()
		j.WindowTruncateTo = spec.WindowConfig().GetTruncateTo()
	}

	return j
}

func fromJobProtos(protoJobSpecs []*pb.JobSpecification) ([]*job.Spec, []job.Name, error) {
	me := errors.NewMultiError("adapting specs errors")
	var jobSpecs []*job.Spec
	var jobNameWithValidationErrors []job.Name
	for _, jobProto := range protoJobSpecs {
		jobSpec, err := fromJobProto(jobProto)
		if err != nil {
			errorMsg := fmt.Sprintf("job %s not passed validation: %s", jobProto.Name, err.Error())
			me.Append(errors.NewError(errors.ErrInternalError, job.EntityJob, errorMsg))

			jobNameWithValidationError, err := job.NameFrom(jobProto.Name)
			if err == nil {
				jobNameWithValidationErrors = append(jobNameWithValidationErrors, jobNameWithValidationError)
			}
			continue
		}
		jobSpecs = append(jobSpecs, jobSpec)
	}
	return jobSpecs, jobNameWithValidationErrors, me.ToErr()
}

func fromJobProto(js *pb.JobSpecification) (*job.Spec, error) {
	version := int(js.Version)

	name, err := job.NameFrom(js.Name)
	if err != nil {
		return nil, err
	}

	owner := js.Owner

	startDate, err := job.ScheduleDateFrom(js.StartDate)
	if err != nil {
		return nil, err
	}

	scheduleBuilder := job.NewScheduleBuilder(startDate).
		WithCatchUp(js.CatchUp).
		WithDependsOnPast(js.DependsOnPast).
		WithInterval(js.Interval)

	if js.EndDate != "" {
		endDate, err := job.ScheduleDateFrom(js.EndDate)
		if err != nil {
			return nil, err
		}
		scheduleBuilder = scheduleBuilder.WithEndDate(endDate)
	}

	var alerts []*job.AlertSpec
	var webhook []*job.WebhookSpec
	if js.Behavior != nil {
		if js.Behavior.Retry != nil {
			retry := toRetry(js.Behavior.Retry)
			scheduleBuilder = scheduleBuilder.WithRetry(retry)
		}
		if js.Behavior.Notify != nil {
			alerts, err = toAlerts(js.Behavior.Notify)
			if err != nil {
				return nil, err
			}
		}
		if js.Behavior.Webhook != nil {
			webhook, err = toWebhook(js.Behavior.Webhook)
			if err != nil {
				return nil, err
			}
		}
	}

	schedule, err := scheduleBuilder.Build()
	if err != nil {
		return nil, err
	}

	window, err := toWindow(js)
	if err != nil {
		return nil, err
	}

	task, err := toTask(js)
	if err != nil {
		return nil, err
	}

	jobSpecBuilder := job.NewSpecBuilder(version, name, owner, schedule, window, task).WithDescription(js.Description)

	if js.Labels != nil {
		labels := labels.FromMap(js.Labels)
		jobSpecBuilder = jobSpecBuilder.WithLabels(labels)
	}

	if js.Hooks != nil {
		hooks, err := toHooks(js.Hooks)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithHooks(hooks)
	}

	if alerts != nil {
		jobSpecBuilder = jobSpecBuilder.WithAlerts(alerts)
	}
	if webhook != nil {
		jobSpecBuilder = jobSpecBuilder.WithWebhooks(webhook)
	}

	if js.Dependencies != nil {
		upstream, err := toSpecUpstreams(js.Dependencies)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithSpecUpstream(upstream)
	}

	if js.Metadata != nil {
		metadata, err := toMetadata(js.Metadata)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithMetadata(metadata)
	}

	if js.Assets != nil {
		asset, err := job.AssetFrom(js.Assets)
		if err != nil {
			return nil, err
		}
		jobSpecBuilder = jobSpecBuilder.WithAsset(asset)
	}

	return jobSpecBuilder.Build()
}

func fromResourceURNs(resourceURNs []resource.URN) []string {
	var resources []string
	for _, resourceURN := range resourceURNs {
		resources = append(resources, resourceURN.String())
	}
	return resources
}

func fromRetryAndAlerts(jobRetry *job.Retry, alerts []*job.AlertSpec) *pb.JobSpecification_Behavior {
	retryProto := fromRetry(jobRetry)
	notifierProto := fromAlerts(alerts)
	if retryProto == nil && len(notifierProto) == 0 {
		return nil
	}
	return &pb.JobSpecification_Behavior{
		Retry:  retryProto,
		Notify: notifierProto,
	}
}

func toTask(js *pb.JobSpecification) (job.Task, error) {
	var err error
	if js.Task == nil { // Old flow
		t1, err := job.TaskNameFrom(js.TaskName)
		if err != nil {
			return job.Task{}, err
		}
		var taskConfig job.Config
		if js.Config != nil {
			taskConfig, err = toConfig(js.Config)
			if err != nil {
				return job.Task{}, err
			}
		}

		return job.NewTask(t1, taskConfig, ""), err
	}

	var taskConfig job.Config
	if js.Task.Config != nil {
		taskConfig, err = toConfig(js.Task.Config)
		if err != nil {
			return job.Task{}, err
		}
	}
	taskName, err := job.TaskNameFrom(js.Task.Name)
	if err != nil {
		return job.Task{}, err
	}
	task := job.NewTask(taskName, taskConfig, js.Task.Version)
	return task, nil
}

func toWindow(js *pb.JobSpecification) (window.Config, error) {
	if js.Window != nil {
		w := js.Window

		if w.Preset != "" {
			return window.NewPresetConfig(w.Preset)
		}
		if js.Version == window.NewWindowVersion {
			return window.NewConfig(w.Size, w.ShiftBy, w.Location, w.TruncateTo)
		}
	}

	if js.WindowPreset != "" {
		return window.NewPresetConfig(js.WindowPreset)
	}

	if js.Version < window.NewWindowVersion {
		w, err := models.NewWindow(int(js.Version), js.WindowTruncateTo, js.WindowOffset, js.WindowSize)
		if err != nil {
			return window.Config{}, err
		}
		if err := w.Validate(); err != nil {
			return window.Config{}, err
		}
		return window.NewCustomConfig(w), nil
	}
	return window.NewIncrementalConfig(), nil
}

func toRetry(protoRetry *pb.JobSpecification_Behavior_Retry) *job.Retry {
	if protoRetry == nil {
		return nil
	}
	return job.NewRetry(int(protoRetry.Count), protoRetry.Delay.GetSeconds(), protoRetry.ExponentialBackoff)
}

func fromRetry(jobRetry *job.Retry) *pb.JobSpecification_Behavior_Retry {
	if jobRetry == nil {
		return nil
	}
	return &pb.JobSpecification_Behavior_Retry{
		Count:              int32(jobRetry.Count()),
		Delay:              &durationpb.Duration{Seconds: jobRetry.DelayInSeconds()},
		ExponentialBackoff: jobRetry.ExponentialBackoff(),
	}
}

func toHooks(hooksProto []*pb.JobSpecHook) ([]*job.Hook, error) {
	hooks := make([]*job.Hook, len(hooksProto))
	for i, hookProto := range hooksProto {
		hookConfig, err := toConfig(hookProto.Config)
		if err != nil {
			return nil, err
		}
		hookSpec, err := job.NewHook(hookProto.Name, hookConfig, hookProto.Version)
		if err != nil {
			return nil, err
		}
		hooks[i] = hookSpec
	}
	return hooks, nil
}

func fromHooks(hooks []*job.Hook) []*pb.JobSpecHook {
	var hooksProto []*pb.JobSpecHook
	for _, hook := range hooks {
		hooksProto = append(hooksProto, &pb.JobSpecHook{
			Name:    hook.Name(),
			Version: hook.Version(),
			Config:  fromConfig(hook.Config()),
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

func toAlerts(notifiers []*pb.JobSpecification_Behavior_Notifiers) ([]*job.AlertSpec, error) {
	alerts := make([]*job.AlertSpec, len(notifiers))
	for i, notify := range notifiers {
		alertOn := utils.FromEnumProto(notify.On.String(), "type")
		config, err := job.ConfigFrom(notify.Config)
		if err != nil {
			return nil, err
		}
		alertConfig, err := job.NewAlertSpec(alertOn, notify.Channels, config, notify.GetSeverity(), notify.GetTeam())
		if err != nil {
			return nil, err
		}
		alerts[i] = alertConfig
	}
	return alerts, nil
}

func toWebhook(webhookNotifiers []*pb.JobSpecification_Behavior_Webhook) ([]*job.WebhookSpec, error) {
	webhooks := make([]*job.WebhookSpec, len(webhookNotifiers))
	for i, webhook := range webhookNotifiers {
		webhookConfig := &job.WebhookSpec{
			On:        utils.FromEnumProto(webhook.On.String(), "type"),
			Endpoints: make([]job.WebhookEndPoint, len(webhook.Endpoints)),
		}

		for i2, endpoint := range webhook.Endpoints {
			headers, err := job.ConfigFrom(endpoint.Headers)
			if err != nil {
				return nil, err
			}
			webhookConfig.Endpoints[i2] = job.WebhookEndPoint{
				URL:     endpoint.Url,
				Headers: headers,
			}
		}

		webhooks[i] = webhookConfig
	}
	return webhooks, nil
}

func fromAlerts(jobAlerts []*job.AlertSpec) []*pb.JobSpecification_Behavior_Notifiers {
	var notifiers []*pb.JobSpecification_Behavior_Notifiers
	for _, alert := range jobAlerts {
		notifiers = append(notifiers, &pb.JobSpecification_Behavior_Notifiers{
			On:       pb.JobEvent_Type(pb.JobEvent_Type_value[utils.ToEnumProto(alert.On(), "type")]),
			Channels: alert.Channels(),
			Config:   alert.Config(),
			Severity: alert.Severity(),
		})
	}
	return notifiers
}

func toSpecUpstreams(upstreamProtos []*pb.JobDependency) (*job.UpstreamSpec, error) {
	var upstreamNames []job.SpecUpstreamName
	var httpUpstreams []*job.SpecHTTPUpstream
	for _, upstream := range upstreamProtos {
		upstreamName := job.SpecUpstreamNameFrom(upstream.Name)
		if upstream.HttpDependency == nil {
			upstreamNames = append(upstreamNames, upstreamName)
			continue
		}
		httpUpstreamProto := upstream.HttpDependency
		httpUpstream, err := job.NewSpecHTTPUpstreamBuilder(httpUpstreamProto.Name, httpUpstreamProto.Url).
			WithHeaders(httpUpstreamProto.Headers).
			WithParams(httpUpstreamProto.Params).
			Build()
		if err != nil {
			return nil, err
		}
		httpUpstreams = append(httpUpstreams, httpUpstream)
	}
	upstream, err := job.NewSpecUpstreamBuilder().WithUpstreamNames(upstreamNames).WithSpecHTTPUpstream(httpUpstreams).Build()
	if err != nil {
		return nil, err
	}
	return upstream, nil
}

func fromSpecUpstreams(upstreams *job.UpstreamSpec) []*pb.JobDependency {
	if upstreams == nil {
		return nil
	}
	var dependencies []*pb.JobDependency
	for _, upstreamName := range upstreams.UpstreamNames() {
		dependencies = append(dependencies, &pb.JobDependency{Name: upstreamName.String()}) // TODO: upstream type?
	}
	for _, httpUpstream := range upstreams.HTTPUpstreams() {
		dependencies = append(dependencies, &pb.JobDependency{
			HttpDependency: &pb.HttpDependency{
				Name:    httpUpstream.Name(),
				Url:     httpUpstream.URL(),
				Headers: httpUpstream.Headers(),
				Params:  httpUpstream.Params(),
			},
		})
	}
	return dependencies
}

func toMetadata(jobMetadata *pb.JobMetadata) (*job.Metadata, error) {
	metadataBuilder := job.NewMetadataBuilder()

	if jobMetadata.Resource != nil {
		metadataResourceProto := jobMetadata.Resource
		var request *job.MetadataResourceConfig
		if metadataResourceProto.Request != nil {
			request = job.NewMetadataResourceConfig(metadataResourceProto.Request.Cpu, metadataResourceProto.Request.Memory)
		}
		var limit *job.MetadataResourceConfig
		if metadataResourceProto.Limit != nil {
			limit = job.NewMetadataResourceConfig(metadataResourceProto.Limit.Cpu, metadataResourceProto.Limit.Memory)
		}
		resourceMetadata := job.NewResourceMetadata(request, limit)
		metadataBuilder = metadataBuilder.WithResource(resourceMetadata)
	}

	if jobMetadata.Airflow != nil {
		metadataSchedulerProto := jobMetadata.Airflow
		schedulerMetadata := map[string]string{
			"pool":  metadataSchedulerProto.Pool,
			"queue": metadataSchedulerProto.Queue,
		}
		metadataBuilder = metadataBuilder.WithScheduler(schedulerMetadata)
	}
	metadata, err := metadataBuilder.Build()
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

func fromMetadata(metadata *job.Metadata) *pb.JobMetadata {
	if metadata == nil {
		return nil
	}

	metadataResourceProto := &pb.JobSpecMetadataResource{}
	if metadata.Resource() != nil {
		if metadata.Resource().Request() != nil {
			metadataResourceProto.Request = &pb.JobSpecMetadataResourceConfig{
				Cpu:    metadata.Resource().Request().CPU(),
				Memory: metadata.Resource().Request().Memory(),
			}
		}
		if metadata.Resource().Limit() != nil {
			metadataResourceProto.Limit = &pb.JobSpecMetadataResourceConfig{
				Cpu:    metadata.Resource().Limit().CPU(),
				Memory: metadata.Resource().Limit().Memory(),
			}
		}
	}

	metadataSchedulerProto := &pb.JobSpecMetadataAirflow{}
	if metadata.Scheduler() != nil {
		scheduler := metadata.Scheduler()
		if _, ok := scheduler["pool"]; ok {
			metadataSchedulerProto.Pool = metadata.Scheduler()["pool"]
		}
		if _, ok := scheduler["queue"]; ok {
			metadataSchedulerProto.Queue = metadata.Scheduler()["queue"]
		}
	}
	return &pb.JobMetadata{
		Resource: metadataResourceProto,
		Airflow:  metadataSchedulerProto,
	}
}

func toConfig(configs []*pb.JobConfigItem) (job.Config, error) {
	configMap := make(map[string]string, len(configs))
	for _, config := range configs {
		configMap[config.Name] = config.Value
	}
	return job.ConfigFrom(configMap)
}

func fromConfig(jobConfig job.Config) []*pb.JobConfigItem {
	configs := []*pb.JobConfigItem{}
	for configName, configValue := range jobConfig {
		configs = append(configs, &pb.JobConfigItem{Name: configName, Value: configValue})
	}
	return configs
}

func toBasicInfoSectionProto(jobDetail *job.Job, logMessages []*pb.Log) *pb.JobInspectResponse_BasicInfoSection {
	var sources []string
	for _, source := range jobDetail.Sources() {
		sources = append(sources, source.String())
	}
	return &pb.JobInspectResponse_BasicInfoSection{
		Destination: jobDetail.Destination().String(),
		Source:      sources,
		Job:         ToJobProto(jobDetail),
		Notice:      logMessages,
	}
}

func toUpstreamProtos(upstreams []*job.Upstream, upstreamSpec *job.UpstreamSpec, upstreamLogs []*pb.Log) *pb.JobInspectResponse_UpstreamSection {
	var internalUpstreamProtos []*pb.JobInspectResponse_JobDependency
	var externalUpstreamProtos []*pb.JobInspectResponse_JobDependency
	var unknownUpstreamProtos []*pb.JobInspectResponse_UpstreamSection_UnknownDependencies
	for _, upstream := range upstreams {
		if upstream.State() != job.UpstreamStateResolved {
			unknownUpstreamProtos = append(unknownUpstreamProtos, &pb.JobInspectResponse_UpstreamSection_UnknownDependencies{
				JobName:             upstream.Name().String(),
				ProjectName:         upstream.ProjectName().String(),
				ResourceDestination: upstream.Resource().String(),
			})
			continue
		}
		upstreamProto := &pb.JobInspectResponse_JobDependency{
			Name:          upstream.Name().String(),
			Host:          upstream.Host(),
			ProjectName:   upstream.ProjectName().String(),
			NamespaceName: upstream.NamespaceName().String(),
			TaskName:      upstream.TaskName().String(),
		}
		if upstream.External() {
			externalUpstreamProtos = append(externalUpstreamProtos, upstreamProto)
		} else {
			internalUpstreamProtos = append(internalUpstreamProtos, upstreamProto)
		}
	}

	var httpUpstreamProto []*pb.HttpDependency
	if upstreamSpec != nil {
		httpUpstreamProto = toHTTPUpstreamProtos(upstreamSpec.HTTPUpstreams())
	}

	return &pb.JobInspectResponse_UpstreamSection{
		ExternalDependency:  externalUpstreamProtos,
		InternalDependency:  internalUpstreamProtos,
		HttpDependency:      httpUpstreamProto,
		UnknownDependencies: unknownUpstreamProtos,
		Notice:              upstreamLogs,
	}
}

func toJobChangeLogProto(changeLog *job.ChangeLog) *pb.JobChangelog {
	pbChange := &pb.JobChangelog{
		EventType: changeLog.Type,
		Timestamp: changeLog.Time.String(),
	}
	pbChange.Change = make([]*pb.JobChange, len(changeLog.Change))
	for i, change := range changeLog.Change {
		pbChange.Change[i] = &pb.JobChange{
			AttributeName: change.Property,
			Diff:          change.Diff,
		}
	}
	return pbChange
}

func toHTTPUpstreamProtos(httpUpstreamSpecs []*job.SpecHTTPUpstream) []*pb.HttpDependency {
	var httpUpstreamProtos []*pb.HttpDependency
	for _, httpUpstream := range httpUpstreamSpecs {
		httpUpstreamProtos = append(httpUpstreamProtos, &pb.HttpDependency{
			Name:    httpUpstream.Name(),
			Url:     httpUpstream.URL(),
			Headers: httpUpstream.Headers(),
			Params:  httpUpstream.Params(),
		})
	}
	return httpUpstreamProtos
}

func toDownstreamProtos(downstreamJobs []*job.Downstream) []*pb.JobInspectResponse_JobDependency {
	var downstreamProtos []*pb.JobInspectResponse_JobDependency
	for _, downstreamJob := range downstreamJobs {
		downstreamProtos = append(downstreamProtos, &pb.JobInspectResponse_JobDependency{
			Name:          downstreamJob.Name().String(),
			ProjectName:   downstreamJob.ProjectName().String(),
			NamespaceName: downstreamJob.NamespaceName().String(),
			TaskName:      downstreamJob.TaskName().String(),
		})
	}
	return downstreamProtos
}
