package github_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/git/github"
)

func TestAPI_getOwnerAndRepoName(t *testing.T) {
	type fields struct {
		baseURL, token string
	}
	type args struct {
		projectID any
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantOwner string
		wantRepo  string
		wantErr   bool
	}{
		{
			name:      "success with valid owner and repo",
			args:      args{projectID: "goto/optimus"},
			wantOwner: "goto",
			wantRepo:  "optimus",
			wantErr:   false,
		},
		{
			name:    "return error when invalid project id",
			args:    args{projectID: "1231412"},
			wantErr: true,
		},
		{
			name:    "return error when invalid segment project id",
			args:    args{projectID: "gotooptimus"},
			wantErr: true,
		},
		{
			name:    "return error when invalid project id type",
			args:    args{projectID: int64(10000)},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ap, err := github.NewGithub(tt.fields.baseURL, tt.fields.token)
			assert.NoError(t, err)
			assert.NotNil(t, ap)

			gotOwner, gotRepo, err := ap.GetOwnerAndRepoName(tt.args.projectID)
			assert.Equal(t, gotOwner, tt.wantOwner)
			assert.Equal(t, gotRepo, tt.wantRepo)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
