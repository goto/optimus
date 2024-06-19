package github_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/google/go-github/v59/github"
	"github.com/stretchr/testify/assert"

	mock_github "github.com/goto/optimus/client/extension/mock/provider/github"
	githubapi "github.com/goto/optimus/client/extension/provider/github"
)

const defaultPerPage = 100

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
			ap, err := githubapi.NewAPI(tt.fields.baseURL, tt.fields.token)
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

func TestAPI_CompareDiff(t *testing.T) {
	var (
		ctx         = context.Background()
		owner       = "goto"
		repo        = "optimus"
		projectID   = owner + "/" + repo
		source      = "feat-optimus"
		destination = "main"
		resp        = &github.Response{NextPage: 0}
	)

	toPtr := func(s string) *string { return &s }

	t.Run("success get comparison diff", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		pagination := &github.ListOptions{
			Page:    1,
			PerPage: defaultPerPage,
		}
		compareResp := &github.CommitsComparison{
			Files: []*github.CommitFile{
				{Filename: toPtr("go.mod")},
				nil,
			},
		}
		mockRepo.On("CompareCommits", ctx, owner, repo, destination, source, pagination).
			Return(compareResp, resp, nil)

		diff, err := api.CompareDiff(ctx, projectID, destination, source)
		assert.NoError(t, err)
		assert.Len(t, diff, 1)
		assert.Empty(t, diff[0].OldPath)
		assert.Equal(t, diff[0].NewPath, "go.mod")
	})

	t.Run("return error when get compare commits", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		pagination := &github.ListOptions{
			Page:    1,
			PerPage: defaultPerPage,
		}
		mockRepo.On("CompareCommits", ctx, owner, repo, destination, source, pagination).
			Return(nil, resp, context.DeadlineExceeded)

		diff, err := api.CompareDiff(ctx, projectID, destination, source)
		assert.Error(t, err)
		assert.Empty(t, diff)
	})

	t.Run("return error when invalid project id", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		diff, err := api.CompareDiff(ctx, "project-id-1", destination, source)
		assert.Error(t, err)
		assert.Empty(t, diff)
	})
}

func TestAPI_GetFileContent(t *testing.T) {
	var (
		ctx             = context.Background()
		owner           = "goto"
		repo            = "optimus"
		projectID       = owner + "/" + repo
		ref             = "main"
		filePath        = "go.mod"
		expectedContent = []byte("module github.com/goto/optimus\n\ngo 1.20\n")
	)

	toPtr := func(s string) *string { return &s }

	t.Run("success get file content", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		repoContent := &github.RepositoryContent{
			Encoding: toPtr("base64"),
			Content:  toPtr(base64.StdEncoding.EncodeToString(expectedContent)),
		}
		mockRepo.On("GetContents", ctx, owner, repo, filePath, &github.RepositoryContentGetOptions{Ref: ref}).
			Return(repoContent, nil, nil, nil)

		actualContent, err := api.GetFileContent(ctx, projectID, ref, filePath)
		assert.NoError(t, err)
		assert.Equal(t, actualContent, expectedContent)
	})

	t.Run("return error when call GetContents", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		mockRepo.On("GetContents", ctx, owner, repo, filePath, &github.RepositoryContentGetOptions{Ref: ref}).
			Return(nil, nil, &github.Response{Response: &http.Response{StatusCode: http.StatusRequestTimeout}}, context.DeadlineExceeded)

		diff, err := api.GetFileContent(ctx, projectID, ref, filePath)
		assert.Error(t, err)
		assert.Empty(t, diff)
	})

	t.Run("return nil error and response when not found", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		mockRepo.On("GetContents", ctx, owner, repo, filePath, &github.RepositoryContentGetOptions{Ref: ref}).
			Return(nil, nil, &github.Response{Response: &http.Response{StatusCode: http.StatusNotFound}}, context.DeadlineExceeded)

		diff, err := api.GetFileContent(ctx, projectID, ref, filePath)
		assert.NoError(t, err)
		assert.Empty(t, diff)
	})

	t.Run("return error when invalid project id", func(t *testing.T) {
		mockRepo := mock_github.NewRepository(t)
		api := githubapi.NewGitHubAPI(mockRepo)
		defer mockRepo.AssertExpectations(t)

		diff, err := api.GetFileContent(ctx, "project-id-1", ref, filePath)
		assert.Error(t, err)
		assert.Empty(t, diff)
	})
}
