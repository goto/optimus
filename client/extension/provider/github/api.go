package github

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"

	"github.com/google/go-github/v59/github"

	"github.com/goto/optimus/client/extension/model"
)

type API struct {
	client *github.Client
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
			err = errors.New("unsupported project ID format for github, it should {{owner}}/{{repo}}")
			return
		}
		owner, repo = splitProjectID[0], splitProjectID[1]
		return
	default:
		err = errors.New("unsupported project ID format for github")
		return
	}
}

func (api *API) CompareDiff(ctx context.Context, projectID any, fromRef, toRef string) ([]*model.Diff, error) {
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
		compareResp, _, err = api.client.Repositories.CompareCommits(ctx, owner, repo, toRef, fromRef, pagination)
		if err != nil {
			return nil, err
		}

		compareDiffResp = append(compareDiffResp, compareResp)

		if len(compareResp.Commits) < pagination.PerPage {
			break
		}
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

	repoContent, _, resp, err = api.client.Repositories.GetContents(ctx, owner, repo, fileName, option)
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}

	content, err := repoContent.GetContent()
	return []byte(content), err
}

func NewAPI(baseURL, token string) (*API, error) {
	var (
		api = &API{}
		err error
	)

	api.client = github.NewClient(nil).WithAuthToken(token)
	if baseURL != "" {
		api.client, err = api.client.WithEnterpriseURLs(baseURL, os.Getenv("GIT_UPLOAD_URL"))
	}
	return api, err
}
