package airflow_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/salt/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gocloud.dev/blob"
)

func TestScheduler(t *testing.T) {
	logger := log.NewNoop()

	t.Run("DeployJobs", func(t *testing.T) {
		tnnt, _ := tenant.NewTenant("proj", "ns")
		project, _ := tenant.NewProject("proj", map[string]string{
			tenant.ProjectSchedulerHost:  "host",
			tenant.ProjectStoragePathKey: "path",
		}, map[string]string{})

		t.Run("should return error if bucket creation fails", func(t *testing.T) {
			jobs := []*scheduler.JobWithDetails{}

			bucket := new(mockBucket)
			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, errors.New("bucket creation error")).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, nil, nil, nil)
			err := sch.DeployJobs(context.Background(), tnnt, jobs)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error in creating storage client instance")
		})

		t.Run("should return error if __lib.py failed to be uploaded", func(t *testing.T) {
			jobs := []*scheduler.JobWithDetails{}
			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)
			bucket := new(mockBucket)
			defer bucket.AssertExpectations(t)

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/__lib.py", airflow.SharedLib, mock.Anything).Return(errors.New("upload error")).Once()
			bucket.On("Close").Return(nil).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, nil, nil, nil)
			err := sch.DeployJobs(context.Background(), tnnt, jobs)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error in writing __lib.py file")
		})

		t.Run("should return error if projectGetter fails", func(t *testing.T) {
			jobs := []*scheduler.JobWithDetails{}

			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)
			bucket := new(mockBucket)
			defer bucket.AssertExpectations(t)
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/__lib.py", airflow.SharedLib, mock.Anything).Return(nil).Once()
			bucket.On("Close").Return(nil).Once()
			projectGetter.On("Get", mock.Anything, tnnt.ProjectName()).Return(nil, errors.New("project getter error")).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, nil, projectGetter, nil)
			err := sch.DeployJobs(context.Background(), tnnt, jobs)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "error in getting project details")
		})

		t.Run("should return error if compileAndUpload fails", func(t *testing.T) {
			job := &scheduler.JobWithDetails{
				Job: &scheduler.Job{
					Name:   "job_name",
					Tenant: tnnt,
				},
			}
			jobs := []*scheduler.JobWithDetails{job}

			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)
			bucket := new(mockBucket)
			defer bucket.AssertExpectations(t)
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)
			compiler := new(mockDagCompiler)
			defer compiler.AssertExpectations(t)

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/__lib.py", airflow.SharedLib, mock.Anything).Return(nil).Once()
			bucket.On("Close").Return(nil).Once()
			projectGetter.On("Get", mock.Anything, tnnt.ProjectName()).Return(project, nil).Once()
			compiler.On("Compile", project, job).Return(nil, errors.New("compile error")).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, compiler, projectGetter, nil)
			err := sch.DeployJobs(context.Background(), tnnt, jobs)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "compile error")
		})

		t.Run("should succeed if no errors encountered", func(t *testing.T) {
			job := &scheduler.JobWithDetails{
				Job: &scheduler.Job{
					Name:   "job_name",
					Tenant: tnnt,
				},
				Name: "job_name",
			}
			jobs := []*scheduler.JobWithDetails{job}

			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)
			bucket := new(mockBucket)
			defer bucket.AssertExpectations(t)
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)
			compiler := new(mockDagCompiler)
			defer compiler.AssertExpectations(t)

			compiledJob := []byte("compiled job")

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/__lib.py", airflow.SharedLib, mock.Anything).Return(nil).Once()
			bucket.On("Close").Return(nil).Once()
			projectGetter.On("Get", mock.Anything, tnnt.ProjectName()).Return(project, nil).Once()
			compiler.On("Compile", project, job).Return(compiledJob, nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/ns/job_name.py", compiledJob, mock.Anything).Return(nil).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, compiler, projectGetter, nil)
			err := sch.DeployJobs(context.Background(), tnnt, jobs)
			assert.NoError(t, err)
		})

		t.Run("should only write library once, if 2 or more jobs are uploaded", func(t *testing.T) {
			job1 := &scheduler.JobWithDetails{
				Job: &scheduler.Job{
					Name:   "job_name_1",
					Tenant: tnnt,
				},
				Name: "job_name_1",
			}
			job2 := &scheduler.JobWithDetails{
				Job: &scheduler.Job{
					Name:   "job_name_2",
					Tenant: tnnt,
				},
				Name: "job_name_2",
			}
			compiledJob1 := []byte("compiled job 1")
			compiledJob2 := []byte("compiled job 2")

			bucketFactory := new(mockBucketFactory)
			defer bucketFactory.AssertExpectations(t)
			bucket := new(mockBucket)
			defer bucket.AssertExpectations(t)
			projectGetter := new(mockProjectGetter)
			defer projectGetter.AssertExpectations(t)
			compiler := new(mockDagCompiler)
			defer compiler.AssertExpectations(t)

			bucketFactory.On("New", mock.Anything, tnnt).Return(bucket, nil).Twice()
			bucket.On("Close").Return(nil).Twice()
			projectGetter.On("Get", mock.Anything, tnnt.ProjectName()).Return(project, nil).Twice()
			compiler.On("Compile", project, job1).Return(compiledJob1, nil).Once()
			compiler.On("Compile", project, job2).Return(compiledJob2, nil).Once()

			// should only write once
			bucket.On("WriteAll", mock.Anything, "dags/__lib.py", airflow.SharedLib, mock.Anything).Return(nil).Once()
			// while writing on 2 jobs
			bucket.On("WriteAll", mock.Anything, "dags/ns/job_name_1.py", compiledJob1, mock.Anything).Return(nil).Once()
			bucket.On("WriteAll", mock.Anything, "dags/ns/job_name_2.py", compiledJob2, mock.Anything).Return(nil).Once()

			sch := airflow.NewScheduler(logger, bucketFactory, nil, compiler, projectGetter, nil)
			err := sch.DeployJobs(context.Background(), tnnt, []*scheduler.JobWithDetails{job1})
			assert.NoError(t, err)
			err = sch.DeployJobs(context.Background(), tnnt, []*scheduler.JobWithDetails{job2})
			assert.NoError(t, err)
		})
	})
}

// mockBucket is an autogenerated mock type for the Bucket type
type mockBucket struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *mockBucket) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Delete provides a mock function with given fields: ctx, key
func (_m *mockBucket) Delete(ctx context.Context, key string) error {
	ret := _m.Called(ctx, key)

	if len(ret) == 0 {
		panic("no return value specified for Delete")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string) error); ok {
		r0 = rf(ctx, key)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// List provides a mock function with given fields: opts
func (_m *mockBucket) List(opts *blob.ListOptions) *blob.ListIterator {
	ret := _m.Called(opts)

	if len(ret) == 0 {
		panic("no return value specified for List")
	}

	var r0 *blob.ListIterator
	if rf, ok := ret.Get(0).(func(*blob.ListOptions) *blob.ListIterator); ok {
		r0 = rf(opts)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*blob.ListIterator)
		}
	}

	return r0
}

// WriteAll provides a mock function with given fields: ctx, key, p, opts
func (_m *mockBucket) WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error {
	ret := _m.Called(ctx, key, p, opts)

	if len(ret) == 0 {
		panic("no return value specified for WriteAll")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, string, []byte, *blob.WriterOptions) error); ok {
		r0 = rf(ctx, key, p, opts)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// mockBucketFactory is an autogenerated mock type for the BucketFactory type
type mockBucketFactory struct {
	mock.Mock
}

// New provides a mock function with given fields: ctx, _a1
func (_m *mockBucketFactory) New(ctx context.Context, _a1 tenant.Tenant) (airflow.Bucket, error) {
	ret := _m.Called(ctx, _a1)

	if len(ret) == 0 {
		panic("no return value specified for New")
	}

	var r0 airflow.Bucket
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) (airflow.Bucket, error)); ok {
		return rf(ctx, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.Tenant) airflow.Bucket); ok {
		r0 = rf(ctx, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(airflow.Bucket)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.Tenant) error); ok {
		r1 = rf(ctx, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockDagCompiler is an autogenerated mock type for the DagCompiler type
type mockDagCompiler struct {
	mock.Mock
}

// Compile provides a mock function with given fields: project, job
func (_m *mockDagCompiler) Compile(project *tenant.Project, job *scheduler.JobWithDetails) ([]byte, error) {
	ret := _m.Called(project, job)

	if len(ret) == 0 {
		panic("no return value specified for Compile")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func(*tenant.Project, *scheduler.JobWithDetails) ([]byte, error)); ok {
		return rf(project, job)
	}
	if rf, ok := ret.Get(0).(func(*tenant.Project, *scheduler.JobWithDetails) []byte); ok {
		r0 = rf(project, job)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(*tenant.Project, *scheduler.JobWithDetails) error); ok {
		r1 = rf(project, job)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// mockProjectGetter is an autogenerated mock type for the ProjectGetter type
type mockProjectGetter struct {
	mock.Mock
}

// Get provides a mock function with given fields: _a0, _a1
func (_m *mockProjectGetter) Get(_a0 context.Context, _a1 tenant.ProjectName) (*tenant.Project, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 *tenant.Project
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) (*tenant.Project, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, tenant.ProjectName) *tenant.Project); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*tenant.Project)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, tenant.ProjectName) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
