package gitlab

import (
	"context"
	"fmt"
	"net/http"

	"github.com/xanzy/go-gitlab"

	"github.com/goto/optimus/client/extension/model"
)

const EntityGitlab = "gitlab"

type API struct {
	repository     Repository
	repositoryFile RepositoryFile
	commit         Commit
}

func closeResponse(resp *gitlab.Response) {
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
}

func (api *API) CompareDiff(ctx context.Context, projectID any, target, source string) ([]*model.Diff, error) {
	var (
		compareOption = &gitlab.CompareOptions{
			From:     gitlab.Ptr(target),
			To:       gitlab.Ptr(source),
			Straight: gitlab.Ptr(true),
			Unidiff:  gitlab.Ptr(true),
		}
		compareResp *gitlab.Compare
		err         error
	)

	compareResp, _, err = api.repository.Compare(projectID, compareOption, gitlab.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to compare commits for project %v: %w", projectID, err)
	}

	resp := make([]*model.Diff, 0, len(compareResp.Diffs))
	for _, diff := range compareResp.Diffs {
		if diff == nil {
			continue
		}
		resp = append(resp, &model.Diff{OldPath: diff.OldPath, NewPath: diff.NewPath})
	}

	return resp, nil
}

func (api *API) GetFileContent(ctx context.Context, projectID any, ref, fileName string) ([]byte, error) {
	var (
		option *gitlab.GetRawFileOptions
		resp   *gitlab.Response
		buff   []byte
		err    error
	)

	if ref != "" {
		option = &gitlab.GetRawFileOptions{Ref: gitlab.Ptr(ref)}
	}

	buff, resp, err = api.repositoryFile.GetRawFile(projectID, fileName, option, gitlab.WithContext(ctx))
	defer closeResponse(resp)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get file content for project %v at ref %s and file %s: %w", projectID, ref, fileName, err)
	}

	return buff, nil
}

func (api *API) GetLatestCommitByPath(ctx context.Context, projectID any, path string) (*model.Commit, error) {
	opt := &gitlab.ListCommitsOptions{
		Path:        gitlab.Ptr(path),
		FirstParent: gitlab.Ptr(true), // FirstParent will return merge commit when it was created from MR
	}
	commits, _, err := api.commit.ListCommits(projectID, opt, gitlab.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	var commit *model.Commit
	for _, c := range commits {
		if c == nil {
			continue
		}
		commit = &model.Commit{
			SHA:     c.ID,
			Message: c.Message,
			URL:     c.WebURL,
		}
		break
	}

	if commit == nil {
		return nil, fmt.Errorf("latest commit not found for projectID %v path %s", projectID, path)
	}

	return commit, nil
}

func (api *API) GetCommitDiff(ctx context.Context, projectID any, sha string) ([]*model.Diff, error) {
	var (
		opt = &gitlab.GetCommitDiffOptions{
			Unidiff: gitlab.Ptr(true),
		}
		diffs []*gitlab.Diff
		err   error
	)

	diffs, _, err = api.commit.GetCommitDiff(projectID, sha, opt, gitlab.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("failed to get commit diff for project %v sha %s: %w", projectID, sha, err)
	}

	result := make([]*model.Diff, 0, len(diffs))
	for _, diff := range diffs {
		if diff == nil {
			continue
		}
		result = append(result, &model.Diff{OldPath: diff.OldPath, NewPath: diff.NewPath})
	}

	return result, nil
}

func NewAPI(baseURL, token string) (*API, error) {
	var opts []gitlab.ClientOptionFunc

	if baseURL != "" {
		opts = append(opts, gitlab.WithBaseURL(baseURL))
	}

	client, err := gitlab.NewClient(token, opts...)
	if err != nil {
		return nil, err
	}

	return NewGitLabAPI(client.Repositories, client.RepositoryFiles, client.Commits), nil
}

func NewGitLabAPI(repo Repository, repoFile RepositoryFile, commit Commit) *API {
	return &API{
		repository:     repo,
		repositoryFile: repoFile,
		commit:         commit,
	}
}
