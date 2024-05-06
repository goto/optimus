package gitlab

import (
	"context"
	"net/http"
	"os"

	"github.com/xanzy/go-gitlab"

	"github.com/goto/optimus/client/extension/model"
)

type API struct {
	client *gitlab.Client
}

func (api *API) CompareDiff(ctx context.Context, projectID any, fromRef, toRef string) ([]*model.Diff, error) {
	var (
		compareOption = &gitlab.CompareOptions{
			From:     gitlab.Ptr(fromRef),
			To:       gitlab.Ptr(toRef),
			Straight: gitlab.Ptr(true),
			Unidiff:  gitlab.Ptr(true),
		}
		compareResp *gitlab.Compare
		err         error
	)

	compareResp, _, err = api.client.Repositories.Compare(projectID, compareOption, gitlab.WithContext(ctx))
	if err != nil {
		return nil, err
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

	buff, resp, err = api.client.RepositoryFiles.GetRawFile(projectID, fileName, option, gitlab.WithContext(ctx))
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return nil, nil
		}
		return nil, err
	}
	return buff, nil
}

func NewAPI(baseURL, token string) (*API, error) {
	var (
		opts []gitlab.ClientOptionFunc
		api  = &API{}
		err  error
	)

	if baseURL != "" {
		opts = append(opts, gitlab.WithBaseURL(baseURL))
	}

	if os.Getenv("GIT_PERSONAL") == "true" {
		api.client, err = gitlab.NewClient(token, opts...)
	} else {
		api.client, err = gitlab.NewJobClient(token, opts...)
	}
	return api, err
}
