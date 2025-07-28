package gitlab_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/xanzy/go-gitlab"

	mock_gitlab "github.com/goto/optimus/client/extension/mock/provider/gitlab"
	gitlabapi "github.com/goto/optimus/client/extension/provider/gitlab"
)

func TestAPI(t *testing.T) {
	var (
		ctx           = context.Background()
		projectID     = "goto/optimus"
		source        = "feat-update"
		destination   = "master"
		compareOption = &gitlab.CompareOptions{
			From:     gitlab.Ptr(destination),
			To:       gitlab.Ptr(source),
			Straight: gitlab.Ptr(true),
			Unidiff:  gitlab.Ptr(true),
		}
		ref      = "main"
		filePath = "go.mod"
	)

	t.Run("CompareDiff", func(t *testing.T) {
		t.Run("success get comparison diff", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			compareResp := &gitlab.Compare{Diffs: []*gitlab.Diff{
				{
					NewPath: "go.mod",
					OldPath: "go.mod.sum",
				},
				nil,
			}}
			mockRepo.On("Compare", projectID, compareOption, mock.Anything).
				Return(compareResp, nil, nil)

			actualDiff, err := api.CompareDiff(ctx, projectID, destination, source)
			assert.NoError(t, err)
			assert.Len(t, actualDiff, 1)
			assert.Equal(t, actualDiff[0].OldPath, "go.mod.sum")
			assert.Equal(t, actualDiff[0].NewPath, "go.mod")
		})

		t.Run("return error when call Compare API", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockRepo.On("Compare", projectID, compareOption, mock.Anything).
				Return(nil, nil, context.DeadlineExceeded)

			actualDiff, err := api.CompareDiff(ctx, projectID, destination, source)
			assert.Error(t, err)
			assert.Empty(t, actualDiff)
		})
	})

	t.Run("GetFileContent", func(t *testing.T) {
		expectedContent := []byte("module github.com/goto/optimus\n\ngo 1.20\n")

		t.Run("success get file content", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockRepoFile.On("GetRawFile", projectID, filePath, mock.Anything, mock.Anything).
				Return(expectedContent, nil, nil)

			actualContent, err := api.GetFileContent(ctx, projectID, ref, filePath)
			assert.NoError(t, err)
			assert.Equal(t, actualContent, expectedContent)
		})

		t.Run("return error when call GetContents", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockRepoFile.On("GetRawFile", projectID, filePath, mock.Anything, mock.Anything).
				Return(nil, &gitlab.Response{Response: &http.Response{StatusCode: http.StatusRequestTimeout}}, context.DeadlineExceeded)

			diff, err := api.GetFileContent(ctx, projectID, ref, filePath)
			assert.Error(t, err)
			assert.Empty(t, diff)
		})

		t.Run("return nil error and response when not found", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockRepoFile.On("GetRawFile", projectID, filePath, mock.Anything, mock.Anything).
				Return(nil, &gitlab.Response{Response: &http.Response{StatusCode: http.StatusNotFound}}, context.DeadlineExceeded)

			diff, err := api.GetFileContent(ctx, projectID, ref, filePath)
			assert.NoError(t, err)
			assert.Empty(t, diff)
		})
	})

	t.Run("GetLatestCommitByPath", func(t *testing.T) {
		commits := []*gitlab.Commit{
			{
				ID:      "fdafafaskfasnf",
				Message: "feat: update go.mod",
				WebURL:  "https://gitlab.com/goto/optimus/-/commit/fdafafaskfasnf",
			},
		}
		path := "job/jobA"
		opt := &gitlab.ListCommitsOptions{
			Path:        gitlab.Ptr(path),
			FirstParent: gitlab.Ptr(true),
		}

		t.Run("return commit and nil error when success get latest commit", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockCommit.EXPECT().ListCommits(projectID, opt, mock.Anything).
				Return(commits, nil, nil).
				Once()

			actualCommit, err := api.GetLatestCommitByPath(ctx, projectID, path)
			assert.NoError(t, err)
			assert.Equal(t, actualCommit.SHA, commits[0].ID)
			assert.Equal(t, actualCommit.Message, commits[0].Message)
			assert.Equal(t, actualCommit.URL, commits[0].WebURL)
		})

		t.Run("return nil and error when commit not found", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockCommit.EXPECT().ListCommits(projectID, opt, mock.Anything).
				Return([]*gitlab.Commit{}, nil, nil).
				Once()

			actualCommit, err := api.GetLatestCommitByPath(ctx, projectID, path)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "commit not found for path job/jobA")
			assert.Nil(t, actualCommit)
		})

		t.Run("return nil and error when failed get commit", func(t *testing.T) {
			mockRepo := mock_gitlab.NewRepository(t)
			mockRepoFile := mock_gitlab.NewRepositoryFile(t)
			mockCommit := mock_gitlab.NewCommit(t)
			api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile, mockCommit)
			defer func() {
				mockRepo.AssertExpectations(t)
				mockRepoFile.AssertExpectations(t)
				mockCommit.AssertExpectations(t)
			}()

			mockCommit.EXPECT().ListCommits(projectID, opt, mock.Anything).
				Return([]*gitlab.Commit{}, nil, context.DeadlineExceeded).
				Once()

			actualCommit, err := api.GetLatestCommitByPath(ctx, projectID, path)
			assert.Error(t, err)
			assert.ErrorContains(t, err, context.DeadlineExceeded.Error())
			assert.Nil(t, actualCommit)
		})
	})
}
