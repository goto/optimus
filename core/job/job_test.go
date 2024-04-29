package job_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/lib"
	"github.com/goto/optimus/internal/lib/window"
	"github.com/goto/optimus/internal/models"
)

func TestEntityJob(t *testing.T) {
	project, _ := tenant.NewProject("test-proj",
		map[string]string{
			"bucket":                     "gs://some_folder-2",
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "gs://location",
		})
	namespace, _ := tenant.NewNamespace("test-ns", project.Name(),
		map[string]string{
			"bucket": "gs://ns_bucket",
		})
	sampleTenant, _ := tenant.NewTenant(project.Name().String(), namespace.Name().String())
	jobVersion := 1
	startDate, _ := job.ScheduleDateFrom("2022-10-01")
	jobSchedule, _ := job.NewScheduleBuilder(startDate).Build()
	w, _ := models.NewWindow(jobVersion, "d", "24h", "24h")
	jobWindow := window.NewCustomConfig(w)
	jobTaskConfig, _ := job.ConfigFrom(map[string]string{"sample_task_key": "sample_value"})
	jobTask := job.NewTask("bq2bq", jobTaskConfig)

	specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
	jobADestination, _ := lib.ParseURN("store://project.dataset.sample-a")
	jobASource, _ := lib.ParseURN("store://project.dataset.sample-b")
	jobASources := []lib.URN{jobASource}
	jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

	specB, _ := job.NewSpecBuilder(jobVersion, "job-B", "sample-owner", jobSchedule, jobWindow, jobTask).Build()
	jobBDestination, _ := lib.ParseURN("store://project.dataset.sample-b")
	jobBSource, _ := lib.ParseURN("store://project.dataset.sample-c")
	jobBSources := []lib.URN{jobBSource}
	jobB := job.NewJob(sampleTenant, specB, jobBDestination, jobBSources, false)

	t.Run("GetJobNames", func(t *testing.T) {
		t.Run("should return list of names", func(t *testing.T) {
			expectedJobNames := []job.Name{specA.Name(), specB.Name()}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			jobNames := jobs.GetJobNames()

			assert.EqualValues(t, expectedJobNames, jobNames)
		})
	})
	t.Run("GetNameMap", func(t *testing.T) {
		t.Run("should return map with name as key and spec as value", func(t *testing.T) {
			expectedMap := map[job.Name]*job.Job{
				specA.Name(): jobA,
				specB.Name(): jobB,
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetNameMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("GetFullNameToSpecMap", func(t *testing.T) {
		t.Run("should return map with key full name and value spec", func(t *testing.T) {
			jobs := []*job.Job{jobA, jobB}

			fullNameA := job.FullNameFrom(jobA.ProjectName(), job.Name(jobA.GetName()))
			fullNameB := job.FullNameFrom(jobB.ProjectName(), job.Name(jobB.GetName()))
			expectedMap := map[job.FullName]*job.Spec{
				fullNameA: specA,
				fullNameB: specB,
			}

			actualMap := job.Jobs(jobs).GetFullNameToSpecMap()

			assert.EqualValues(t, expectedMap[fullNameA], actualMap[fullNameA])
			assert.EqualValues(t, expectedMap[fullNameB], actualMap[fullNameB])
		})
	})

	t.Run("GetNamespaceNameAndJobsMap", func(t *testing.T) {
		t.Run("should return map with namespace name as key and jobs as value", func(t *testing.T) {
			expectedMap := map[tenant.NamespaceName][]*job.Job{
				namespace.Name(): {jobA, jobB},
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetNamespaceNameAndJobsMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})
	t.Run("GetSpecs", func(t *testing.T) {
		t.Run("should return job specifications", func(t *testing.T) {
			expectedSpecs := []*job.Spec{
				jobA.Spec(),
				jobB.Spec(),
			}

			jobs := job.Jobs([]*job.Job{jobA, jobB})
			resultMap := jobs.GetSpecs()

			assert.EqualValues(t, expectedSpecs, resultMap)
		})
	})
	t.Run("GetUnresolvedUpstreams", func(t *testing.T) {
		t.Run("should return upstreams with state unresolved", func(t *testing.T) {
			upstreamUnresolved1 := job.NewUpstreamUnresolvedStatic("job-B", project.Name())
			resourceURNC, _ := lib.ParseURN("store://project.dataset.sample-c")
			upstreamUnresolved2 := job.NewUpstreamUnresolvedInferred(resourceURNC)
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			upstreamResolved := job.NewUpstreamResolved("job-d", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeStatic, "", false)

			expected := []*job.Upstream{upstreamUnresolved1, upstreamUnresolved2}

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamUnresolved1, upstreamResolved, upstreamUnresolved2})

			unresolvedUpstreams := jobAWithUpstream.GetUnresolvedUpstreams()
			assert.EqualValues(t, expected, unresolvedUpstreams)
		})
	})
	t.Run("GetResolvedUpstreams", func(t *testing.T) {
		t.Run("should return upstreams with state resolved", func(t *testing.T) {
			upstreamUnresolved1 := job.NewUpstreamUnresolvedStatic("job-B", project.Name())
			resourceURNC, _ := lib.ParseURN("store://project.dataset.sample-c")
			upstreamUnresolved2 := job.NewUpstreamUnresolvedInferred(resourceURNC)
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			resourceURNE, _ := lib.ParseURN("store://project.dataset.sample-e")
			upstreamResolved1 := job.NewUpstreamResolved("job-d", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-e", "host-sample", resourceURNE, sampleTenant, job.UpstreamTypeInferred, "", true)

			expected := []*job.Upstream{upstreamResolved1, upstreamResolved2}

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamUnresolved1, upstreamResolved1, upstreamResolved2, upstreamUnresolved2})

			resolvedUpstreams := jobAWithUpstream.GetResolvedUpstreams()
			assert.EqualValues(t, expected, resolvedUpstreams)
		})
	})
	t.Run("UpstreamTypeFrom", func(t *testing.T) {
		t.Run("should create static upstream type from string", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("static")
			assert.NoError(t, err)
			assert.Equal(t, job.UpstreamTypeStatic, upstreamType)
		})
		t.Run("should create inferred upstream type from string", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("inferred")
			assert.NoError(t, err)
			assert.Equal(t, job.UpstreamTypeInferred, upstreamType)
		})
		t.Run("should return error if the input is invalid", func(t *testing.T) {
			upstreamType, err := job.UpstreamTypeFrom("unrecognized type")
			assert.Empty(t, upstreamType)
			assert.ErrorContains(t, err, "unknown type for upstream")
		})
	})

	t.Run("ToFullNameAndUpstreamMap", func(t *testing.T) {
		t.Run("should return a map with full name as key and boolean as value", func(t *testing.T) {
			resourceURNA, _ := lib.ParseURN("store://project.dataset.sample-a")
			resourceURNB, _ := lib.ParseURN("store://project.dataset.sample-b")
			upstreamResolved1 := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", resourceURNB, sampleTenant, job.UpstreamTypeInferred, "", false)

			expectedMap := map[string]*job.Upstream{
				"test-proj/job-a": upstreamResolved1,
				"test-proj/job-b": upstreamResolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{upstreamResolved1, upstreamResolved2})
			resultMap := upstreams.ToFullNameAndUpstreamMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("ToResourceDestinationAndUpstreamMap", func(t *testing.T) {
		t.Run("should return a map with destination resource urn as key and boolean as value", func(t *testing.T) {
			resourceURNA, _ := lib.ParseURN("store://project.dataset.sample-a")
			resourceURNB, _ := lib.ParseURN("store://project.dataset.sample-b")
			upstreamResolved1 := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", resourceURNB, sampleTenant, job.UpstreamTypeInferred, "", false)

			expectedMap := map[string]*job.Upstream{
				"store://project.dataset.sample-a": upstreamResolved1,
				"store://project.dataset.sample-b": upstreamResolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{upstreamResolved1, upstreamResolved2})
			resultMap := upstreams.ToResourceDestinationAndUpstreamMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
		t.Run("should skip a job if resource destination is not found and should not return error", func(t *testing.T) {
			var zeroURN lib.URN
			resourceURNB, _ := lib.ParseURN("store://project.dataset.sample-b")

			upstreamResolved1 := job.NewUpstreamResolved("job-a", "host-sample", zeroURN, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", resourceURNB, sampleTenant, job.UpstreamTypeInferred, "", false)

			expectedMap := map[string]*job.Upstream{
				"store://project.dataset.sample-b": upstreamResolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{upstreamResolved1, upstreamResolved2})
			resultMap := upstreams.ToResourceDestinationAndUpstreamMap()

			assert.EqualValues(t, expectedMap, resultMap)
		})
	})

	t.Run("Deduplicate", func(t *testing.T) {
		t.Run("should return upstreams with static being prioritized if duplication is found", func(t *testing.T) {
			resourceURNA, _ := lib.ParseURN("store://project.dataset.sample-a")
			resourceURNB, _ := lib.ParseURN("store://project.dataset.sample-b")
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			upstreamResolved1Inferred := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeInferred, "", false)
			upstreamResolved1Static := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", resourceURNB, sampleTenant, job.UpstreamTypeInferred, "", false)
			upstreamUnresolved1 := job.NewUpstreamUnresolvedStatic("job-c", sampleTenant.ProjectName())
			upstreamUnresolved2 := job.NewUpstreamUnresolvedInferred(resourceURND)
			upstreamUnresolved3 := job.NewUpstreamUnresolvedStatic("job-c", sampleTenant.ProjectName())
			upstreamUnresolved4 := job.NewUpstreamUnresolvedInferred(resourceURND)

			expected := []*job.Upstream{
				upstreamResolved1Static,
				upstreamResolved2,
				upstreamUnresolved1,
				upstreamUnresolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{
				upstreamResolved1Inferred,
				upstreamResolved1Static,
				upstreamResolved2,
				upstreamUnresolved1,
				upstreamUnresolved2,
				upstreamUnresolved3,
				upstreamUnresolved4,
			})
			result := upstreams.Deduplicate()

			assert.ElementsMatch(t, expected, result)
		})
		t.Run("should successfully return distinct upstreams when only resolved upstream is present", func(t *testing.T) {
			resourceURNA, _ := lib.ParseURN("store://project.dataset.sample-a")
			resourceURNB, _ := lib.ParseURN("store://project.dataset.sample-b")
			upstreamResolved1Inferred := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeInferred, "", false)
			upstreamResolved1Static := job.NewUpstreamResolved("job-a", "host-sample", resourceURNA, sampleTenant, job.UpstreamTypeStatic, "", false)
			upstreamResolved2 := job.NewUpstreamResolved("job-b", "host-sample", resourceURNB, sampleTenant, job.UpstreamTypeInferred, "", false)

			expected := []*job.Upstream{
				upstreamResolved1Static,
				upstreamResolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{
				upstreamResolved1Inferred,
				upstreamResolved1Static,
				upstreamResolved2,
			})
			result := upstreams.Deduplicate()

			assert.ElementsMatch(t, expected, result)
		})
		t.Run("should successfully return distinct upstreams when only unresolved upstream is present", func(t *testing.T) {
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			upstreamUnresolved1 := job.NewUpstreamUnresolvedStatic("job-c", sampleTenant.ProjectName())
			upstreamUnresolved2 := job.NewUpstreamUnresolvedInferred(resourceURND)
			upstreamUnresolved3 := job.NewUpstreamUnresolvedStatic("job-c", sampleTenant.ProjectName())
			upstreamUnresolved4 := job.NewUpstreamUnresolvedInferred(resourceURND)

			expected := []*job.Upstream{
				upstreamUnresolved1,
				upstreamUnresolved2,
			}

			upstreams := job.Upstreams([]*job.Upstream{
				upstreamUnresolved1,
				upstreamUnresolved2,
				upstreamUnresolved3,
				upstreamUnresolved4,
			})
			result := upstreams.Deduplicate()

			assert.ElementsMatch(t, expected, result)
		})
	})

	t.Run("FullNameFrom", func(t *testing.T) {
		t.Run("should return the job full name given project and job name", func(t *testing.T) {
			fullName := job.FullNameFrom(project.Name(), specA.Name())
			assert.Equal(t, job.FullName("test-proj/job-A"), fullName)
			assert.Equal(t, "test-proj/job-A", fullName.String())
		})
	})

	t.Run("FullNames", func(t *testing.T) {
		t.Run("String() should return joined full names", func(t *testing.T) {
			names := []job.FullName{"proj1/job-A", "proj2/job-B", "proj1/job-C"}

			expectedNames := "proj1/job-A, proj2/job-B, proj1/job-C"

			assert.Equal(t, expectedNames, job.FullNames(names).String())
		})
	})

	t.Run("Job", func(t *testing.T) {
		t.Run("should return values as inserted", func(t *testing.T) {
			specUpstream, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"job-E"}).Build()
			specC, _ := job.NewSpecBuilder(jobVersion, "job-C", "sample-owner", jobSchedule, jobWindow, jobTask).WithSpecUpstream(specUpstream).Build()
			jobCDestination, _ := lib.ParseURN("store://project.dataset.sample-c")
			jobCSource, _ := lib.ParseURN("store://project.dataset.sample-c")
			jobCSources := []lib.URN{jobCSource}
			jobC := job.NewJob(sampleTenant, specC, jobCDestination, jobCSources, false)

			assert.Equal(t, sampleTenant, jobC.Tenant())
			assert.Equal(t, specC, jobC.Spec())
			assert.Equal(t, jobCSources, jobC.Sources())
			assert.Equal(t, jobCDestination, jobC.Destination())
			assert.Equal(t, project.Name(), jobC.ProjectName())
			assert.Equal(t, specC.Name().String(), jobC.GetName())
			assert.Equal(t, "test-proj/job-C", jobC.FullName())
			assert.Equal(t, specUpstream.UpstreamNames(), jobC.StaticUpstreamNames())
		})

		t.Run("StaticUpstreamNames", func(t *testing.T) {
			t.Run("should return nil if no static upstream spec specified", func(t *testing.T) {
				assert.Nil(t, jobA.StaticUpstreamNames())
			})
		})
	})

	t.Run("WithUpstream", func(t *testing.T) {
		t.Run("should return values as constructed", func(t *testing.T) {
			resourceURNC, _ := lib.ParseURN("store://project.dataset.sample-c")
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			upstreamResolved := job.NewUpstreamResolved("job-d", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeStatic, "bq2bq", false)
			assert.Equal(t, job.Name("job-d"), upstreamResolved.Name())
			assert.Equal(t, "host-sample", upstreamResolved.Host())
			assert.Equal(t, resourceURND, upstreamResolved.Resource())
			assert.Equal(t, job.UpstreamTypeStatic, upstreamResolved.Type())
			assert.Equal(t, job.UpstreamStateResolved, upstreamResolved.State())
			assert.Equal(t, project.Name(), upstreamResolved.ProjectName())
			assert.Equal(t, namespace.Name(), upstreamResolved.NamespaceName())
			assert.Equal(t, false, upstreamResolved.External())
			assert.Equal(t, job.TaskName("bq2bq"), upstreamResolved.TaskName())
			assert.Equal(t, "test-proj/job-d", upstreamResolved.FullName())

			upstreamUnresolved := job.NewUpstreamUnresolvedInferred(resourceURNC)

			jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamResolved, upstreamUnresolved})
			assert.Equal(t, jobA, jobAWithUpstream.Job())
			assert.EqualValues(t, []*job.Upstream{upstreamResolved, upstreamUnresolved}, jobAWithUpstream.Upstreams())
			assert.EqualValues(t, specA.Name(), jobAWithUpstream.Name())
		})
	})

	t.Run("WithUpstreams", func(t *testing.T) {
		t.Run("GetSubjectJobNames", func(t *testing.T) {
			t.Run("should return job names of WithUpstream list", func(t *testing.T) {
				resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
				upstreamResolved := job.NewUpstreamResolved("job-d", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeStatic, "bq2bq", false)
				jobAWithUpstream := job.NewWithUpstream(jobA, []*job.Upstream{upstreamResolved})
				jobBWithUpstream := job.NewWithUpstream(jobB, []*job.Upstream{upstreamResolved})
				jobsWithUpstream := []*job.WithUpstream{jobAWithUpstream, jobBWithUpstream}
				result := job.WithUpstreams(jobsWithUpstream).GetSubjectJobNames()

				assert.EqualValues(t, []job.Name{"job-A", "job-B"}, result)
			})
		})
		t.Run("MergeWithResolvedUpstreams", func(t *testing.T) {
			resourceURNC, _ := lib.ParseURN("store://project.dataset.sample-c")
			resourceURND, _ := lib.ParseURN("store://project.dataset.sample-d")
			resourceURNF, _ := lib.ParseURN("store://project.dataset.sample-f")
			upstreamCUnresolved := job.NewUpstreamUnresolvedInferred(resourceURNC)
			upstreamDUnresolvedInferred := job.NewUpstreamUnresolvedInferred(resourceURND)
			upstreamDUnresolvedStatic := job.NewUpstreamUnresolvedStatic("job-D", project.Name())
			upstreamEUnresolved := job.NewUpstreamUnresolvedStatic("job-E", project.Name())
			upstreamFUnresolved := job.NewUpstreamUnresolvedInferred(resourceURNF)

			upstreamCResolved := job.NewUpstreamResolved("job-C", "host-sample", resourceURNC, sampleTenant, job.UpstreamTypeInferred, "bq2bq", false)
			upstreamDResolvedStatic := job.NewUpstreamResolved("job-D", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeStatic, "bq2bq", false)
			upstreamDResolvedInferred := job.NewUpstreamResolved("job-D", "host-sample", resourceURND, sampleTenant, job.UpstreamTypeInferred, "bq2bq", false)

			resolvedUpstreamMap := map[job.Name][]*job.Upstream{
				"job-A": {upstreamCResolved, upstreamDResolvedInferred},
				"job-B": {upstreamDResolvedStatic},
			}

			expected := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{upstreamCResolved, upstreamDResolvedInferred, upstreamEUnresolved}),
				job.NewWithUpstream(jobB, []*job.Upstream{upstreamDResolvedStatic, upstreamEUnresolved, upstreamFUnresolved}),
			}

			jobsWithUnresolvedUpstream := []*job.WithUpstream{
				job.NewWithUpstream(jobA, []*job.Upstream{upstreamCUnresolved, upstreamDUnresolvedInferred, upstreamEUnresolved}),
				job.NewWithUpstream(jobB, []*job.Upstream{upstreamDUnresolvedStatic, upstreamDUnresolvedInferred, upstreamEUnresolved, upstreamFUnresolved}),
			}

			result := job.WithUpstreams(jobsWithUnresolvedUpstream).MergeWithResolvedUpstreams(resolvedUpstreamMap)
			assert.Equal(t, expected[0].Job(), result[0].Job())
			assert.Equal(t, expected[1].Job(), result[1].Job())
			assert.ElementsMatch(t, expected[0].Upstreams(), result[0].Upstreams())
			assert.ElementsMatch(t, expected[1].Upstreams(), result[1].Upstreams())
		})
	})

	t.Run("Downstream", func(t *testing.T) {
		t.Run("should return value as constructed", func(t *testing.T) {
			downstream := job.NewDownstream(specA.Name(), project.Name(), namespace.Name(), jobTask.Name())
			assert.Equal(t, specA.Name(), downstream.Name())
			assert.Equal(t, project.Name(), downstream.ProjectName())
			assert.Equal(t, namespace.Name(), downstream.NamespaceName())
			assert.Equal(t, jobTask.Name(), downstream.TaskName())
			assert.Equal(t, job.FullName("test-proj/job-A"), downstream.FullName())
		})
	})

	t.Run("GetDownstreamFullNames", func(t *testing.T) {
		t.Run("should return full names of downstream list", func(t *testing.T) {
			downstreamA := job.NewDownstream(specA.Name(), project.Name(), namespace.Name(), jobTask.Name())
			downstreamB := job.NewDownstream(specB.Name(), project.Name(), namespace.Name(), jobTask.Name())
			downstreamList := []*job.Downstream{downstreamA, downstreamB}

			expectedFullNames := []job.FullName{"test-proj/job-A", "test-proj/job-B"}

			result := job.DownstreamList(downstreamList).GetDownstreamFullNames()
			assert.EqualValues(t, expectedFullNames, result)
		})
	})

	t.Run("GetJobWithUnresolvedUpstream", func(t *testing.T) {
		t.Run("should contains error when get static upstream failed because of job name empty", func(t *testing.T) {
			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobWithUpstream, err := jobA.GetJobWithUnresolvedUpstream()
			assert.ErrorContains(t, err, "failed to get static upstreams to resolve")
			assert.Len(t, jobWithUpstream.Upstreams(), 1)
		})
		t.Run("should contains error when get static upstream failed because of project name empty", func(t *testing.T) {
			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"/job-C"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobWithUpstream, err := jobA.GetJobWithUnresolvedUpstream()
			assert.ErrorContains(t, err, "failed to get static upstreams to resolve")
			assert.Len(t, jobWithUpstream.Upstreams(), 1)
		})
		t.Run("should get unresolved upstream", func(t *testing.T) {
			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-C"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)

			jobWithUpstreams, err := jobA.GetJobWithUnresolvedUpstream()
			assert.NoError(t, err)
			assert.Len(t, jobWithUpstreams.Upstreams(), 2)
		})
	})

	t.Run("GetJobsWithUnresolvedUpstreams", func(t *testing.T) {
		t.Run("should return with error when individual get job with upstream has error", func(t *testing.T) {
			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"/job-C"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)
			jobs := job.Jobs([]*job.Job{jobA, jobB})

			jobsWithUpstreams, err := jobs.GetJobsWithUnresolvedUpstreams()
			assert.Error(t, err)
			assert.Len(t, jobsWithUpstreams, 2)
		})
		t.Run("should get unresolved upstreams", func(t *testing.T) {
			jobTaskPython := job.NewTask("python", jobTaskConfig)
			upstreamsSpecA, _ := job.NewSpecUpstreamBuilder().WithUpstreamNames([]job.SpecUpstreamName{"test-proj/job-C"}).Build()
			specA, _ := job.NewSpecBuilder(jobVersion, "job-A", "sample-owner", jobSchedule, jobWindow, jobTaskPython).WithSpecUpstream(upstreamsSpecA).Build()
			jobA := job.NewJob(sampleTenant, specA, jobADestination, jobASources, false)
			jobs := job.Jobs([]*job.Job{jobA, jobB})

			jobsWithUpstreams, err := jobs.GetJobsWithUnresolvedUpstreams()
			assert.NoError(t, err)
			assert.Len(t, jobsWithUpstreams, 2)
		})
	})
}
