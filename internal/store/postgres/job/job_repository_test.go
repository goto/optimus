//go:build !unit_test

package job_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/resource"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib/window"
	postgres "github.com/goto/optimus/internal/store/postgres/job"
	tenantPostgres "github.com/goto/optimus/internal/store/postgres/tenant"
	"github.com/goto/optimus/tests/setup"
)

func TestPostgresJobRepository(t *testing.T) {
	ctx := context.Background()

	proj, err := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		}, map[string]string{})
	assert.NoError(t, err)

	otherProj, err := tenant.NewProject("test-other-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-3",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		}, map[string]string{})
	assert.NoError(t, err)

	namespace, err := tenant.NewNamespace("test-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		}, map[string]string{})
	assert.NoError(t, err)

	otherNamespace, err := tenant.NewNamespace("other-ns", proj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		}, map[string]string{})
	assert.NoError(t, err)

	otherNamespace2, err := tenant.NewNamespace("other-ns", otherProj.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		}, map[string]string{})
	assert.NoError(t, err)
	sampleTenant, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
	assert.NoError(t, err)

	resourceURNA, err := resource.ParseURN("store://dev.resource.sample_a")
	assert.NoError(t, err)
	resourceURNB, err := resource.ParseURN("store://dev.resource.sample_b")
	assert.NoError(t, err)
	resourceURNC, err := resource.ParseURN("store://dev.resource.sample_c")
	assert.NoError(t, err)
	resourceURND, err := resource.ParseURN("store://dev.resource.sample_d")
	assert.NoError(t, err)
	resourceURNE, err := resource.ParseURN("store://dev.resource.sample_e")
	assert.NoError(t, err)
	resourceURNF, err := resource.ParseURN("store://dev.resource.sample_f")
	assert.NoError(t, err)
	resourceURNG, err := resource.ParseURN("store://dev.resource.sample_g")
	assert.NoError(t, err)

	resourceURNX, err := resource.ParseURN("store://dev.resource.sample_x")
	assert.NoError(t, err)
	resourceURNY, err := resource.ParseURN("store://dev.resource.sample_y")
	assert.NoError(t, err)

	resourceURN3, err := resource.ParseURN("store://dev.resource.sample_3")
	assert.NoError(t, err)
	resourceURN4, err := resource.ParseURN("store://dev.resource.sample_4")
	assert.NoError(t, err)

	dbSetup := func() *pgxpool.Pool {
		pool := setup.TestPool()
		setup.TruncateTablesWith(pool)
		projRepo := tenantPostgres.NewProjectRepository(pool)
		assert.NoError(t, projRepo.Save(ctx, proj))
		assert.NoError(t, projRepo.Save(ctx, otherProj))

		namespaceRepo := tenantPostgres.NewNamespaceRepository(pool)
		assert.NoError(t, namespaceRepo.Save(ctx, namespace))
		assert.NoError(t, namespaceRepo.Save(ctx, otherNamespace))
		assert.NoError(t, namespaceRepo.Save(ctx, otherNamespace2))

		return pool
	}

	jobVersion := 1
	assert.NoError(t, err)
	jobOwner := "dev_test"
	assert.NoError(t, err)
	jobDescription := "sample job"
	jobRetry := job.NewRetry(5, 0, false)
	startDate, err := job.ScheduleDateFrom("2022-10-01")
	assert.NoError(t, err)
	jobSchedule, err := job.NewScheduleBuilder(startDate).WithRetry(jobRetry).Build()
	assert.NoError(t, err)
	customConfig, err := window.NewConfig("1d", "1d", "", "")
	assert.NoError(t, err)
	jobTaskConfig, err := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	assert.NoError(t, err)
	taskName, err := job.TaskNameFrom("bq2bq")
	assert.NoError(t, err)
	jobTask := job.NewTask(taskName, jobTaskConfig, "")

	host := "sample-host"
	upstreamType := job.UpstreamTypeInferred

	t.Run("Add", func(t *testing.T) {
		t.Run("inserts job spec", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.ConfigFrom(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHook1, err := job.NewHook("sample_hook", jobHookConfig, "")
			assert.NoError(t, err)
			jobHooks := []*job.Hook{jobHook1}
			jobAlertConfig, err := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, err := job.NewAlertSpec("sla_miss", []string{"sample-channel"}, jobAlertConfig, "", "")
			assert.NoError(t, err)
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			assert.NoError(t, err)
			jobAsset, err := job.AssetFrom(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, err := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()
			assert.NoError(t, err)

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobA.SetState(job.ENABLED.String())
			jobB.SetState(job.ENABLED.String())

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			storedJobs, err := jobRepo.GetAllByProjectName(ctx, proj.Name())
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, storedJobs)
		})
		t.Run("inserts job spec with optional fields empty", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)
		})
		t.Run("skip job and return job error if job already exist", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURN3}, false)

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "already exists")
			assert.EqualValues(t, []*job.Job{jobB}, addedJobs)
		})
		t.Run("return error if all jobs are failed to be saved", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURN3}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			addedJobs, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.ErrorContains(t, err, "already exists")
			assert.Nil(t, addedJobs)
		})
		t.Run("update job spec if the job is already exist but soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.ConfigFrom(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHook1, err := job.NewHook("sample_hook", jobHookConfig, "")
			assert.NoError(t, err)
			jobHooks := []*job.Hook{jobHook1}
			jobAlertConfig, err := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, err := job.NewAlertSpec("sla_miss", []string{"sample-channel"}, jobAlertConfig, "", "")
			assert.NoError(t, err)
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			assert.NoError(t, err)
			jobAsset, err := job.AssetFrom(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, err := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()
			assert.NoError(t, err)

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobA.Spec().Name(), false)
			assert.NoError(t, err)

			addedJobs, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Job{jobA}, addedJobs)
		})
		t.Run("avoid re-inserting job if it is soft deleted in other namespace", func(t *testing.T) {
			db := dbSetup()

			jobLabels := map[string]string{
				"environment": "integration",
			}
			jobHookConfig, err := job.ConfigFrom(map[string]string{"sample_hook_key": "sample_value"})
			assert.NoError(t, err)
			jobHook1, err := job.NewHook("sample_hook", jobHookConfig, "")
			assert.NoError(t, err)
			jobHooks := []*job.Hook{jobHook1}
			jobAlertConfig, err := job.ConfigFrom(map[string]string{"sample_alert_key": "sample_value"})
			assert.NoError(t, err)
			alert, err := job.NewAlertSpec("sla_miss", []string{"sample-channel"}, jobAlertConfig, "", "")
			assert.NoError(t, err)
			jobAlerts := []*job.AlertSpec{alert}
			upstreamName1 := job.SpecUpstreamNameFrom("job-upstream-1")
			upstreamName2 := job.SpecUpstreamNameFrom("job-upstream-2")
			jobUpstream, err := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName1, upstreamName2}).Build()
			assert.NoError(t, err)
			jobAsset, err := job.AssetFrom(map[string]string{"sample-asset": "value-asset"})
			assert.NoError(t, err)
			resourceRequestConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceLimitConfig := job.NewMetadataResourceConfig("250m", "128Mi")
			resourceMetadata := job.NewResourceMetadata(resourceRequestConfig, resourceLimitConfig)
			jobMetadata, err := job.NewMetadataBuilder().
				WithResource(resourceMetadata).
				WithScheduler(map[string]string{"scheduler_config_key": "value"}).
				Build()
			assert.NoError(t, err)

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithLabels(jobLabels).
				WithHooks(jobHooks).
				WithAlerts(jobAlerts).
				WithSpecUpstream(jobUpstream).
				WithAsset(jobAsset).
				WithMetadata(jobMetadata).
				Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobA.Spec().Name(), false)
			assert.NoError(t, err)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)

			jobAToReAdd := job.NewJob(otherTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)
			addedJobs, err = jobRepo.Add(ctx, []*job.Job{jobAToReAdd})
			assert.ErrorContains(t, err, "already exists and soft deleted in namespace test-ns")
			assert.Nil(t, addedJobs)
		})
	})

	t.Run("Update", func(t *testing.T) {
		t.Run("updates job spec", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobs := []*job.Job{jobA, jobB}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			jobSpecAToUpdate, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				Build()
			assert.NoError(t, err)

			jobAToUpdate := job.NewJob(sampleTenant, jobSpecAToUpdate, resourceURNA, []resource.URN{resourceURN3}, false)
			jobBToUpdate := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURN4}, false)
			jobsToUpdate := []*job.Job{jobAToUpdate, jobBToUpdate}

			updatedJobs, err := jobRepo.Update(ctx, jobsToUpdate)
			assert.NoError(t, err)
			assert.EqualValues(t, jobsToUpdate, updatedJobs)
		})
		t.Run("skip job and return job error if job not exist yet", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			jobSpecAToUpdate, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				Build()
			assert.NoError(t, err)
			jobAToUpdate := job.NewJob(sampleTenant, jobSpecAToUpdate, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobBToUpdate := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURN4}, false)
			jobsToUpdate := []*job.Job{jobAToUpdate, jobBToUpdate}

			updatedJobs, err := jobRepo.Update(ctx, jobsToUpdate)
			assert.ErrorContains(t, err, "not exists yet")
			assert.EqualValues(t, []*job.Job{jobAToUpdate}, updatedJobs)
		})
		t.Run("return error if all jobs are failed to be updated", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURN3}, false)

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Update(ctx, []*job.Job{jobA, jobB})
			assert.Error(t, err)
			assert.Nil(t, addedJobs)
		})
		t.Run("should not update job if it has been soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			updatedJobs, err := jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "update is not allowed as job sample-job-A has been soft deleted")
			assert.Nil(t, updatedJobs)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)
			jobToUpdate := job.NewJob(otherTenant, jobSpecA, resource.ZeroURN(), nil, false)
			updatedJobs, err = jobRepo.Update(ctx, []*job.Job{jobToUpdate})
			assert.ErrorContains(t, err, "already exists and soft deleted in namespace test-ns")
			assert.Nil(t, updatedJobs)
		})
		t.Run("should not update job if it is owned by different namespace", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			jobs := []*job.Job{jobA}

			jobRepo := postgres.NewJobRepository(db)
			addedJobs, err := jobRepo.Add(ctx, jobs)
			assert.NoError(t, err)
			assert.EqualValues(t, jobs, addedJobs)

			otherTenant, err := tenant.NewTenant(proj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)
			jobAToUpdate := job.NewJob(otherTenant, jobSpecA, resourceURNA, []resource.URN{resourceURN3}, false)

			updatedJobs, err := jobRepo.Update(ctx, []*job.Job{jobAToUpdate})
			assert.ErrorContains(t, err, "job sample-job-A already exists in namespace test-ns")
			assert.Nil(t, updatedJobs)
		})
	})

	t.Run("ResolveUpstreams", func(t *testing.T) {
		t.Run("returns job with inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tnnt, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.NoError(t, err)

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tnnt, "inferred", taskName, false)

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static upstreams", func(t *testing.T) {
			db := dbSetup()

			tnnt, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.NoError(t, err)

			upstreamName := job.SpecUpstreamNameFrom("sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			jobSpecA, _ := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, nil, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			expectedUpstream := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tnnt, "static", taskName, false)

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.Equal(t, expectedUpstream, upstreams[jobSpecA.Name()][0])
		})
		t.Run("returns job with static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			tenantDetails, err := tenant.NewTenantDetails(proj, namespace, nil)
			assert.NoError(t, err)

			upstreamName := job.SpecUpstreamNameFrom("test-proj/sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			jobSpecA, _ := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobSpecC, err := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNC, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tenantDetails.ToTenant(), "static", taskName, false)
			upstreamC := job.NewUpstreamResolved(jobSpecC.Name(), "", jobC.Destination(), tenantDetails.ToTenant(), "inferred", taskName, false)

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamC,
			}

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
		t.Run("returns job with external project static and inferred upstreams", func(t *testing.T) {
			db := dbSetup()

			otherTenant, err := tenant.NewTenant(otherProj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)

			upstreamDName := job.SpecUpstreamNameFrom("test-other-proj/sample-job-D")

			upstreamBName := job.SpecUpstreamNameFrom("test-proj/sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamBName, upstreamDName}).Build()
			jobSpecA, _ := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC, resourceURNE}, false)

			// internal project, same server
			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)
			jobSpecC, err := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNC, nil, false)

			// external project, same server
			jobSpecD, _ := job.NewSpecBuilder(jobVersion, "sample-job-D", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			jobD := job.NewJob(otherTenant, jobSpecD, resourceURND, nil, false)
			jobSpecE, _ := job.NewSpecBuilder(jobVersion, "sample-job-E", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			jobE := job.NewJob(otherTenant, jobSpecE, resourceURNE, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC, jobD, jobE})
			assert.NoError(t, err)

			upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), sampleTenant, "static", taskName, false)
			upstreamC := job.NewUpstreamResolved(jobSpecC.Name(), "", jobC.Destination(), sampleTenant, "inferred", taskName, false)
			upstreamD := job.NewUpstreamResolved(jobSpecD.Name(), "", jobD.Destination(), otherTenant, "static", taskName, false)
			upstreamE := job.NewUpstreamResolved(jobSpecE.Name(), "", jobE.Destination(), otherTenant, "inferred", taskName, false)

			expectedUpstreams := []*job.Upstream{
				upstreamB,
				upstreamD,
				upstreamC,
				upstreamE,
			}

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
		t.Run("returns job with static upstream if found duplicated upstream from static and inferred", func(t *testing.T) {
			db := dbSetup()

			tnnt, err := tenant.NewTenant(proj.Name().String(), namespace.Name().String())
			assert.NoError(t, err)

			upstreamName := job.SpecUpstreamNameFrom("test-proj/sample-job-B")
			jobAUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{upstreamName}).Build()
			jobSpecA, _ := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).
				WithDescription(jobDescription).
				WithSpecUpstream(jobAUpstream).
				Build()
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			upstreamB := job.NewUpstreamResolved(jobSpecB.Name(), "", jobB.Destination(), tnnt, "static", taskName, false)

			expectedUpstreams := []*job.Upstream{
				upstreamB,
			}

			upstreams, err := jobRepo.ResolveUpstreams(ctx, proj.Name(), []job.Name{jobSpecA.Name()})
			assert.NoError(t, err)
			assert.ElementsMatch(t, expectedUpstreams, upstreams[jobSpecA.Name()])
		})
	})

	t.Run("ReplaceUpstreams", func(t *testing.T) {
		jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
		assert.NoError(t, err)
		jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)

		jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
		assert.NoError(t, err)
		jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

		jobSpecC, err := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
		assert.NoError(t, err)
		jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNC, nil, false)

		t.Run("inserts job upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, resourceURNB, sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, resourceURNC, sampleTenant, upstreamType, taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err := jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("inserts job upstreams including unresolved upstreams", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, resourceURNB, sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, resourceURNC, sampleTenant, upstreamType, taskName, false)
			upstreamD := job.NewUpstreamUnresolvedInferred(resourceURND)
			upstreams := []*job.Upstream{upstreamB, upstreamC, upstreamD}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err := jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)
			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
		t.Run("deletes existing job upstream and inserts", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, resourceURNB, sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, resourceURNC, sampleTenant, upstreamType, taskName, false)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err = jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream}))

			upstreamsOfJobA, err := jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, upstreamsOfJobA)

			jobAWithUpstream = job.NewWithUpstream(jobA, []*job.Upstream{upstreamC})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream}))

			upstreamsOfJobA, err = jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamC}, upstreamsOfJobA)
		})
		t.Run("deletes existing job upstream without inserts if no longer upstream found", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, resourceURNB, sampleTenant, upstreamType, taskName, false)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err = jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamB})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream}))

			upstreamsOfJobA, err := jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{upstreamB}, upstreamsOfJobA)

			jobAWithNoUpstream := job.NewWithUpstream(jobA, []*job.Upstream{})
			assert.NoError(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithNoUpstream}))

			upstreamsOfJobA, err = jobUpstreamRepo.GetUpstreams(ctx, proj.Name(), jobA.Spec().Name())
			assert.NoError(t, err)
			assert.Empty(t, upstreamsOfJobA)
		})
		t.Run("inserts job upstreams with exact name across projects exists", func(t *testing.T) {
			db := dbSetup()

			upstreamB := job.NewUpstreamResolved("jobB", host, resourceURNB, sampleTenant, upstreamType, taskName, false)
			upstreamC := job.NewUpstreamResolved("jobC", host, resourceURNC, sampleTenant, upstreamType, taskName, false)
			upstreams := []*job.Upstream{upstreamB, upstreamC}
			jobWithUpstream := job.NewWithUpstream(jobA, upstreams)

			jobUpstreamRepo := postgres.NewJobRepository(db)
			_, err := jobUpstreamRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			otherTenant, err := tenant.NewTenant(otherProj.Name().String(), otherNamespace.Name().String())
			assert.NoError(t, err)

			otherProjectJobA := job.NewJob(otherTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)
			otherProjectJobB := job.NewJob(otherTenant, jobSpecB, resourceURNB, nil, false)
			_, err = jobUpstreamRepo.Add(ctx, []*job.Job{otherProjectJobA, otherProjectJobB})
			assert.NoError(t, err)

			assert.Nil(t, jobUpstreamRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobWithUpstream}))
		})
	})
	t.Run("ChangeJobNamespace", func(t *testing.T) {
		newTenant, _ := tenant.NewTenant(otherNamespace.ProjectName().String(), otherNamespace.Name().String())
		jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
		assert.NoError(t, err)
		jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)

		jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
		assert.NoError(t, err)
		jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNA}, false)

		t.Run("Change Job namespace successfully", func(t *testing.T) {
			db := dbSetup()

			jobRepo := postgres.NewJobRepository(db)
			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			upstreamAInferred := job.NewUpstreamResolved("sample-job-A", "host-1", resourceURNA, sampleTenant, "inferred", taskName, false)
			jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamAInferred})
			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobBWithUpstream})
			assert.NoError(t, err)

			// update failure with proper log message shows job has been soft deleted
			err = jobRepo.ChangeJobNamespace(ctx, jobSpecA.Name(), sampleTenant, newTenant)
			assert.Nil(t, err)

			jobA, err = jobRepo.GetByJobName(ctx, proj.Name(), jobSpecA.Name())
			assert.Nil(t, err)
			assert.Equal(t, jobA.Tenant().NamespaceName(), newTenant.NamespaceName())

			jobBUpstreams, err := jobRepo.GetUpstreams(ctx, proj.Name(), jobSpecB.Name())
			assert.Nil(t, err)
			jobAIsUpstream := false
			for _, upstream := range jobBUpstreams {
				if upstream.Name() == jobSpecA.Name() {
					jobAIsUpstream = true
					assert.Equal(t, upstream.NamespaceName(), newTenant.NamespaceName())
				}
			}
			assert.True(t, jobAIsUpstream)
		})
	})

	t.Run("Delete", func(t *testing.T) {
		t.Run("soft delete a job if not asked to do clean delete", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, nil, false)

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			// update failure with proper log message shows job has been soft deleted
			_, err = jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "update is not allowed as job sample-job-A has been soft deleted")
		})
		t.Run("should return error if the soft delete failed", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)

			jobRepo := postgres.NewJobRepository(db)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), false)
			assert.ErrorContains(t, err, "failed to be deleted")
		})
		t.Run("hard delete a job if asked to do clean delete", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, nil, false)

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.NoError(t, err)

			// update failure with proper log message shows job has been hard deleted
			_, err = jobRepo.Update(ctx, []*job.Job{jobA})
			assert.ErrorContains(t, err, "job sample-job-A not exists yet")
		})
		t.Run("should return error if the hard delete failed", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)

			jobRepo := postgres.NewJobRepository(db)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.ErrorContains(t, err, "failed to be deleted")
		})
		t.Run("do delete job and delete upstream relationship", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB}, false)

			jobSpecX, err := job.NewSpecBuilder(jobVersion, "sample-job-X", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobX := job.NewJob(sampleTenant, jobSpecX, resourceURNX, []resource.URN{resourceURNA}, false)

			jobRepo := postgres.NewJobRepository(db)

			addedJob, err := jobRepo.Add(ctx, []*job.Job{jobA, jobX})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			upstreamBInferred := job.NewUpstreamResolved("sample-job-B", "host-1", resourceURNB, sampleTenant, "inferred", taskName, false)
			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamBInferred})
			upstreamAInferred := job.NewUpstreamResolved("sample-job-A", "host-1", resourceURNA, sampleTenant, "inferred", taskName, false)
			jobXWithUpstream := job.NewWithUpstream(jobX, []*job.Upstream{upstreamAInferred})

			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream, jobXWithUpstream})
			assert.NoError(t, err)

			upstreams, err := jobRepo.GetUpstreams(ctx, proj.Name(), jobSpecX.Name())
			assert.Equal(t, 1, len(upstreams))
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, proj.Name(), jobSpecA.Name(), true)
			assert.NoError(t, err)

			// should succeed adding as job already cleaned earlier
			addedJob, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			assert.NotNil(t, addedJob)

			// data in upstream should already be deleted
			upstreams, err = jobRepo.GetUpstreams(ctx, proj.Name(), jobSpecX.Name())
			assert.Equal(t, 0, len(upstreams))
			assert.NoError(t, err)
		})
	})

	t.Run("GetByJobName", func(t *testing.T) {
		t.Run("returns job success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)

			actual, err := jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			jobA.SetState(job.ENABLED.String())
			assert.Equal(t, jobA, actual)
		})
		t.Run("should not return job if it is soft deleted", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA})
			assert.NoError(t, err)
			jobA.SetState(job.ENABLED.String())

			actual, err := jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Equal(t, jobA, actual)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecA.Name(), false)
			assert.NoError(t, err)

			actual, err = jobRepo.GetByJobName(ctx, sampleTenant.ProjectName(), "sample-job-A")
			assert.Error(t, err)
			assert.Nil(t, actual)
		})
	})

	t.Run("GetAllByProjectName", func(t *testing.T) {
		t.Run("returns no error when get all jobs success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)
			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)
			jobA.SetState(job.ENABLED.String())
			jobB.SetState(job.ENABLED.String())

			actual, err := jobRepo.GetAllByProjectName(ctx, sampleTenant.ProjectName())
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Len(t, actual, 2)
			assert.Equal(t, []*job.Job{jobA, jobB}, actual)
		})
		t.Run("returns only active jobs excluding the soft deleted jobs", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)
			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, []resource.URN{resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecB.Name(), false)
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByProjectName(ctx, sampleTenant.ProjectName())
			assert.NoError(t, err)
			jobA.SetState(job.ENABLED.String())

			assert.Equal(t, []*job.Job{jobA}, actual)
		})
	})

	t.Run("GetAllByResourceDestination", func(t *testing.T) {
		t.Run("returns no error when get all jobs success", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNY, []resource.URN{resourceURNB, resourceURNC}, false)
			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNY, []resource.URN{resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			jobA.SetState(job.ENABLED.String())
			jobB.SetState(job.ENABLED.String())

			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByResourceDestination(ctx, resourceURNY)
			assert.NoError(t, err)
			assert.NotNil(t, actual)
			assert.Len(t, actual, 2)
			assert.Equal(t, []*job.Job{jobA, jobB}, actual)
		})
		t.Run("returns only active jobs excluding the soft deleted jobs", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNY, []resource.URN{resourceURNB, resourceURNC}, false)
			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNY, []resource.URN{resourceURNC}, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.Delete(ctx, sampleTenant.ProjectName(), jobSpecB.Name(), false)
			assert.NoError(t, err)

			actual, err := jobRepo.GetAllByResourceDestination(ctx, resourceURNY)
			assert.NoError(t, err)
			jobA.SetState(job.ENABLED.String())

			assert.Equal(t, []*job.Job{jobA}, actual)
		})
	})

	t.Run("GetUpstreams", func(t *testing.T) {
		t.Run("returns upstream given project and job name", func(t *testing.T) {
			// TODO: test is failing for nullable fields in upstream
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNY, []resource.URN{resourceURNB, resourceURNC}, false)
			jobAUpstreamResolved := job.NewUpstreamResolved("sample-job-B", "", resource.ZeroURN(), sampleTenant, "inferred", taskName, false)
			jobAUpstreamUnresolved := job.NewUpstreamUnresolvedInferred(resourceURNC)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved})

			jobRepo := postgres.NewJobRepository(db)

			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)

			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream})
			assert.NoError(t, err)

			result, err := jobRepo.GetUpstreams(ctx, proj.Name(), jobSpecA.Name())
			assert.NoError(t, err)
			assert.EqualValues(t, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved}, result)
		})
	})

	t.Run("GetDownstreamByDestination", func(t *testing.T) {
		t.Run("returns downstream given a job destination", func(t *testing.T) {
			db := dbSetup()

			jobAUpstreamSpec, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"sample-job-B"}).Build()
			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).WithSpecUpstream(jobAUpstreamSpec).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNC}, false)

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobSpecC, err := job.NewSpecBuilder(jobVersion, "sample-job-C", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNC, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			expectedDownstream := []*job.Downstream{
				job.NewDownstream(jobSpecA.Name(), proj.Name(), namespace.Name(), jobSpecA.Task().Name()),
			}
			result, err := jobRepo.GetDownstreamByDestination(ctx, proj.Name(), resourceURNC)
			assert.NoError(t, err)
			assert.EqualValues(t, expectedDownstream, result)
		})
	})

	t.Run("GetDownstreamByJobName", func(t *testing.T) {
		t.Run("returns downstream given a job name", func(t *testing.T) {
			db := dbSetup()

			jobSpecA, err := job.NewSpecBuilder(jobVersion, "sample-job-A", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)
			jobAUpstreamResolved := job.NewUpstreamResolved("sample-job-B", "", resource.ZeroURN(), sampleTenant, "inferred", taskName, false)
			jobAUpstreamUnresolved := job.NewUpstreamUnresolvedInferred(resourceURNC)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{jobAUpstreamResolved, jobAUpstreamUnresolved})

			jobSpecB, err := job.NewSpecBuilder(jobVersion, "sample-job-B", jobOwner, jobSchedule, customConfig, jobTask).WithDescription(jobDescription).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURNB, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB})
			assert.NoError(t, err)
			err = jobRepo.ReplaceUpstreams(ctx, []*job.WithUpstream{jobAWithUpstream})
			assert.NoError(t, err)

			expectedDownstream := []*job.Downstream{
				job.NewDownstream(jobSpecA.Name(), proj.Name(), namespace.Name(), jobSpecA.Task().Name()),
			}
			result, err := jobRepo.GetDownstreamByJobName(ctx, proj.Name(), jobSpecB.Name())
			assert.NoError(t, err)
			assert.EqualValues(t, expectedDownstream, result)
		})
	})

	t.Run("GetDownstreamBySources", func(t *testing.T) {
		t.Run("returns empty downstream if resource urns are empty", func(t *testing.T) {
			db := dbSetup()
			jobRepo := postgres.NewJobRepository(db)

			var sources []resource.URN

			result, err := jobRepo.GetDownstreamBySources(ctx, sources)
			assert.NoError(t, err)
			assert.Empty(t, result)
		})

		t.Run("returns downstream given resource urns", func(t *testing.T) {
			db := dbSetup()

			jobAName, _ := job.NameFrom("sample-job-A")
			jobSpecA, err := job.NewSpecBuilder(jobVersion, jobAName, jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobA := job.NewJob(sampleTenant, jobSpecA, resourceURNA, []resource.URN{resourceURNB, resourceURNC}, false)

			jobBName, _ := job.NameFrom("sample-job-b")
			jobSpecB, err := job.NewSpecBuilder(jobVersion, jobBName, jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobB := job.NewJob(sampleTenant, jobSpecB, resourceURND, []resource.URN{resourceURNE}, false)

			jobCName, _ := job.NameFrom("sample-job-c")
			jobSpecC, err := job.NewSpecBuilder(jobVersion, jobCName, jobOwner, jobSchedule, customConfig, jobTask).Build()
			assert.NoError(t, err)
			jobC := job.NewJob(sampleTenant, jobSpecC, resourceURNF, nil, false)

			jobRepo := postgres.NewJobRepository(db)
			_, err = jobRepo.Add(ctx, []*job.Job{jobA, jobB, jobC})
			assert.NoError(t, err)

			jobAAsDownstream := job.NewDownstream(jobAName, sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())
			jobBAsDownstream := job.NewDownstream(jobBName, sampleTenant.ProjectName(), sampleTenant.NamespaceName(), jobTask.Name())

			testCases := []struct {
				sources             []resource.URN
				expectedDownstreams []*job.Downstream
			}{
				{
					sources:             []resource.URN{resourceURNB},
					expectedDownstreams: []*job.Downstream{jobAAsDownstream},
				},
				{
					sources:             []resource.URN{resourceURNB, resourceURNE},
					expectedDownstreams: []*job.Downstream{jobAAsDownstream, jobBAsDownstream},
				},
				{
					sources:             []resource.URN{resourceURNB, resourceURNC},
					expectedDownstreams: []*job.Downstream{jobAAsDownstream},
				},
				{
					sources:             []resource.URN{resourceURNE, resourceURNF},
					expectedDownstreams: []*job.Downstream{jobBAsDownstream},
				},
				{
					sources:             []resource.URN{resourceURNF, resourceURNG},
					expectedDownstreams: nil,
				},
			}

			for _, test := range testCases {
				result, err := jobRepo.GetDownstreamBySources(ctx, test.sources)
				assert.NoError(t, err)
				assert.EqualValues(t, test.expectedDownstreams, result)
			}
		})
	})
}
