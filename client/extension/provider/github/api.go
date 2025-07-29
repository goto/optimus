package github

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/google/go-github/v59/github"

	"github.com/goto/optimus/client/extension/model"
)

type API struct {
	repository Repository
}

const (
	defaultPerPage        = 100
	totalSegmentProjectID = 2
)

func (*API) GetOwnerAndRepoName(projectID any) (owner, repo string, err error) {
	switch value := projectID.(type) {
	case string:
		splitProjectID := strings.SplitN(value, "/", totalSegmentProjectID)
		if len(splitProjectID) != totalSegmentProjectID {
			err = errors.New("unsupported projectID format for github, it should {{owner}}/{{repo}}")
			return
		}
		owner, repo = splitProjectID[0], splitProjectID[1]
		return
	default:
		err = errors.New("unsupported projectID format for github")
		return
	}
}

func (api *API) CompareDiff(ctx context.Context, projectID any, target, source string) ([]*model.Diff, error) {
	var (
		pagination = &github.ListOptions{
			Page:    1,
			PerPage: defaultPerPage,
		}
		compareResp      *github.CommitsComparison
		owner, repo, err = api.GetOwnerAndRepoName(projectID)
	)

	if err != nil {
		return nil, err
	}

	compareDiffResp := make([]*github.CommitsComparison, 0)
	for {
		var resp *github.Response
		compareResp, resp, err = api.repository.CompareCommits(ctx, owner, repo, target, source, pagination)
		if err != nil {
			return nil, fmt.Errorf("failed to compare commits for %s/%s: %w", owner, repo, err)
		}

		compareDiffResp = append(compareDiffResp, compareResp)

		if resp.NextPage == 0 {
			break
		}
		pagination.Page = resp.NextPage
	}

	resp := make([]*model.Diff, 0)
	for i := range compareDiffResp {
		for _, diff := range compareDiffResp[i].Files {
			if diff == nil {
				continue
			}
			resp = append(resp, &model.Diff{
				OldPath: diff.GetPreviousFilename(),
				NewPath: diff.GetFilename(),
			})
		}
	}

	return resp, nil
}

func (api *API) GetFileContent(ctx context.Context, projectID any, ref, fileName string) ([]byte, error) {
	var (
		option           = &github.RepositoryContentGetOptions{Ref: ref}
		resp             *github.Response
		repoContent      *github.RepositoryContent
		owner, repo, err = api.GetOwnerAndRepoName(projectID)
	)

	if err != nil {
		return nil, err
	}

	repoContent, _, resp, err = api.repository.GetContents(ctx, owner, repo, fileName, option)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get file content for %s/%s at ref %s and file %s: %w", owner, repo, ref, fileName, err)
	}

	content, err := repoContent.GetContent()
	return []byte(content), err
}

func (api *API) GetLatestCommitByPath(ctx context.Context, projectID any, path string) (*model.Commit, error) {
	owner, repo, err := api.GetOwnerAndRepoName(projectID)
	if err != nil {
		return nil, err
	}

	opt := &github.CommitsListOptions{
		Path: path,
	}
	commits, _, err := api.repository.ListCommits(ctx, owner, repo, opt)
	if err != nil {
		return nil, fmt.Errorf("failed to list commits for %s/%s at path %s: %w", owner, repo, path, err)
	}

	var commit *model.Commit
	for _, c := range commits {
		if c == nil {
			continue
		}
		commit = &model.Commit{
			SHA: c.GetSHA(),
			URL: c.GetURL(),
		}
		if gitCommit := c.GetCommit(); gitCommit != nil {
			commit.Message = gitCommit.GetMessage()
		}
		break
	}

	if commit == nil {
		return nil, fmt.Errorf("latest commit not found for path %s in project %s", path, projectID)
	}

	return commit, nil
}

func (api *API) GetCommitDiff(ctx context.Context, projectID any, sha string) ([]*model.Diff, error) {
	owner, repo, err := api.GetOwnerAndRepoName(projectID)
	if err != nil {
		return nil, err
	}

	opt := &github.ListOptions{
		PerPage: defaultPerPage,
		Page:    1,
	}
	results := make([]*model.Diff, 0)

	for {
		diff, apiResp, err := api.repository.GetCommit(ctx, owner, repo, sha, opt)
		if err != nil {
			return nil, errors.AddErrContext(err, EntityGitHub, fmt.Sprintf("failed to get commit diff for sha %s", sha))
		}

		if diff == nil || diff.Files == nil {
			break
		}
		opt.Page = apiResp.NextPage

		for _, file := range diff.Files {
			if file == nil {
				continue
			}
			results = append(results, &model.Diff{
				OldPath: file.GetPreviousFilename(),
				NewPath: file.GetFilename(),
			})
		}

		if apiResp.NextPage == 0 {
			break
		}
	}

	return results, nil
}

func NewAPI(baseURL, token string) (*API, error) {
	client := github.NewClient(nil).WithAuthToken(token)
	if baseURL != "" {
		var err error
		client, err = client.WithEnterpriseURLs(baseURL, os.Getenv("GIT_UPLOAD_URL"))
		if err != nil {
			return nil, err
		}
	}

	return NewGitHubAPI(client.Repositories), nil
}

func NewGitHubAPI(repo Repository) *API {
	return &API{repository: repo}
}
