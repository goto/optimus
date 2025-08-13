package plan_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/client/cmd/internal/plan"
)

func TestGetValidJobDirectory(t *testing.T) {
	type args struct {
		directories []string
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
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, plan.GetValidJobDirectory(tt.args.directories), "GetValidJobDirectory(%v)", tt.args.directories)
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

func TestFilterArchiveDirectories(t *testing.T) {
	type args struct {
		directories []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "Success - No archive directories",
			args: args{
				directories: []string{
					"namespace-1/job-A",
					"namespace-1/resource-A",
					"namespace-1/job-B",
				},
			},
			want: []string{
				"namespace-1/job-A",
				"namespace-1/resource-A",
				"namespace-1/job-B",
			},
		},
		{
			name: "Success - Filter archive directory",
			args: args{
				directories: []string{
					"namespace-1/archive",
					"namespace-1/job-A",
					"archive",
					"archive/apple/1.txt",
					"./archive/namespace-1/job.yaml",
					".archive.yaml",
					"./archive.yaml",
					"namespace-2/resource-B",
				},
			},
			want: []string{
				"namespace-1/archive",
				"namespace-1/job-A",
				"namespace-2/resource-B",
			},
		},
		{
			name: "Success - Empty input",
			args: args{
				directories: []string{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, plan.FilterArchiveDirectories(tt.args.directories), "FilterArchiveDirectories(%v)", tt.args.directories)
		})
	}
}
