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

func TestAPI_CompareDiff(t *testing.T) {
	var (
		ctx           = context.Background()
		projectID     = "goto/optimus"
		source        = "feat-update"
		destination   = "master"
		compareOption = &gitlab.CompareOptions{
			From:     gitlab.Ptr(source),
			To:       gitlab.Ptr(destination),
			Straight: gitlab.Ptr(true),
			Unidiff:  gitlab.Ptr(true),
		}
	)

	t.Run("success get comparison diff", func(t *testing.T) {
		mockRepo := mock_gitlab.NewRepository(t)
		mockRepoFile := mock_gitlab.NewRepositoryFile(t)
		api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile)
		defer func() {
			mockRepo.AssertExpectations(t)
			mockRepoFile.AssertExpectations(t)
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

		actualDiff, err := api.CompareDiff(ctx, projectID, source, destination)
		assert.NoError(t, err)
		assert.Len(t, actualDiff, 1)
		assert.Equal(t, actualDiff[0].OldPath, "go.mod.sum")
		assert.Equal(t, actualDiff[0].NewPath, "go.mod")
	})

	t.Run("return error when call Compare API", func(t *testing.T) {
		mockRepo := mock_gitlab.NewRepository(t)
		mockRepoFile := mock_gitlab.NewRepositoryFile(t)
		api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile)
		defer func() {
			mockRepo.AssertExpectations(t)
			mockRepoFile.AssertExpectations(t)
		}()

		mockRepo.On("Compare", projectID, compareOption, mock.Anything).
			Return(nil, nil, context.DeadlineExceeded)

		actualDiff, err := api.CompareDiff(ctx, projectID, source, destination)
		assert.Error(t, err)
		assert.Empty(t, actualDiff)
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

	t.Run("success get file content", func(t *testing.T) {
		mockRepo := mock_gitlab.NewRepository(t)
		mockRepoFile := mock_gitlab.NewRepositoryFile(t)
		api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile)
		defer func() {
			mockRepo.AssertExpectations(t)
			mockRepoFile.AssertExpectations(t)
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
		api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile)
		defer func() {
			mockRepo.AssertExpectations(t)
			mockRepoFile.AssertExpectations(t)
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
		api := gitlabapi.NewGitLabAPI(mockRepo, mockRepoFile)
		defer func() {
			mockRepo.AssertExpectations(t)
			mockRepoFile.AssertExpectations(t)
		}()

		mockRepoFile.On("GetRawFile", projectID, filePath, mock.Anything, mock.Anything).
			Return(nil, &gitlab.Response{Response: &http.Response{StatusCode: http.StatusNotFound}}, context.DeadlineExceeded)

		diff, err := api.GetFileContent(ctx, projectID, ref, filePath)
		assert.NoError(t, err)
		assert.Empty(t, diff)
	})
}
