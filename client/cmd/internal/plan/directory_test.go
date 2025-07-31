package plan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/client/cmd/internal/plan"
)

type mockEmptyJobReader struct {
}

func (j *mockEmptyJobReader) GetPathsByParentPath(_ string) ([]string, error) {
	return []string{}, nil
}

type mockJobParentReader struct {
	jobPaths []string
}

func (j *mockJobParentReader) GetPathsByParentPath(_ string) ([]string, error) {
	return j.jobPaths, nil
}

func NewMockJobParentReader(jobPaths []string) *mockJobParentReader {
	return &mockJobParentReader{
		jobPaths: jobPaths,
	}
}

func TestGetValidJobDirectory(t *testing.T) {
	type args struct {
		directories []string
		reader      plan.JobPathGetter
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Success - All Cases",
			args: args{
				directories: []string{
					"namespace-1/job-A/job.yaml",
					"namespace-1/job-B/assets/query.sql",
				},
				reader: new(mockEmptyJobReader),
			},
			want: []string{
				"namespace-1/job-A",
				"namespace-1/job-B",
			},
		},
		{
			name: "Failed - Not valid job directory",
			args: args{
				directories: []string{
					"namespace-1/job-A/resource.yaml",
				},
				reader: new(mockEmptyJobReader),
			},
			want: nil,
		},
		{
			name: "Success - Valid Job Directory with Parent",
			args: args{
				directories: []string{
					"namespace-1/job-A/this.yaml",
				},
				reader: NewMockJobParentReader(
					[]string{
						"namespace-1/job-A/Z/job.yaml",
						"namespace-1/job-A/Y/job.yaml",
						"namespace-1/job-A/X/assets/query.sql",
					},
				),
			},
			want: []string{
				"namespace-1/job-A/Z",
				"namespace-1/job-A/Y",
				"namespace-1/job-A/X",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, plan.GetValidJobDirectory(tt.args.reader, tt.args.directories), "GetValidJobDirectory(%v)", tt.args.directories)
		})
	}
}

func TestGetValidResourceDirectory(t *testing.T) {
	type args struct {
		directories []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Success - Valid Directory",
			args: args{
				directories: []string{
					"namespace-1/resource-A/resource.yaml",
				},
			},
			want: []string{
				"namespace-1/resource-A",
			},
		},
		{
			name: "Failed - Invalid Directory",
			args: args{
				directories: []string{
					"namespace-1/resource-A/resources.yaml",
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, plan.GetValidResourceDirectory(tt.args.directories), "GetValidResourceDirectory(%v)", tt.args.directories)
		})
	}
}

func TestDistinctDirectory(t *testing.T) {
	type args struct {
		directories []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Success - Distinct Directory",
			args: args{
				directories: []string{
					"namespace-1/job-A",
					"namespace-1/job-A",
					"namespace-1/job-B",
					"namespace-1/resource-A",
					"namespace-1/resource-A",
					"namespace-1/resource-C",
					"namespace-1/resource-C",
					"namespace-1/resource-C",
				},
			},
			want: []string{
				"namespace-1/job-A",
				"namespace-1/job-B",
				"namespace-1/resource-A",
				"namespace-1/resource-C",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, plan.DistinctDirectory(tt.args.directories), "DistinctDirectory(%v)", tt.args.directories)
		})
	}
}
