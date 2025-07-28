package github

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/google/go-github/v59/github"

	"github.com/goto/optimus/client/extension/model"
	"github.com/goto/optimus/internal/errors"
)

type API struct {
	repository Repository
}

const (
	EntityGitHub          = "github"
	defaultPerPage        = 100
	totalSegmentProjectID = 2
)

func (*API) GetOwnerAndRepoName(projectID any) (owner, repo string, err error) {
	switch value := projectID.(type) {
	case string:
		splitProjectID := strings.SplitN(value, "/", totalSegmentProjectID)
		if len(splitProjectID) != totalSegmentProjectID {
			err = errors.InvalidArgument(EntityGitHub, "unsupported project SHA format for github, it should {{owner}}/{{repo}}")
			return
		}
		owner, repo = splitProjectID[0], splitProjectID[1]
		return
	default:
		err = errors.InvalidArgument(EntityGitHub, "unsupported project SHA format for github")
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
			return nil, errors.AddErrContext(err, EntityGitHub, "failed to compare references")
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
		return nil, errors.AddErrContext(err, EntityGitHub, fmt.Sprintf("failed to get file content for %s", fileName))
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
		return nil, errors.AddErrContext(err, EntityGitHub, fmt.Sprintf("failed to list commits for path %s", path))
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
		return nil, errors.NotFound(EntityGitHub, fmt.Sprintf("commit not found for path %s", path))
	}

	return commit, nil
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
