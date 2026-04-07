package audit

import (
	"context"
	"errors"
	"fmt"
	"os"

	gitlab "github.com/xanzy/go-gitlab"

	"github.com/goto/optimus/core/audit"
)

const gitlabAuditSourceKey = "gitlab"

type GitlabAuditSource struct {
	projectID string

	*gitlab.Client
}

func NewGitlabAuditSourceFromEnv() (*GitlabAuditSource, error) {
	gitlabToken := os.Getenv("CI_JOB_TOKEN")
	gitlabHost := os.Getenv("CI_SERVER_URL")
	gitlabProjectID := os.Getenv("CI_PROJECT_ID")

	if gitlabToken == "" || gitlabHost == "" || gitlabProjectID == "" {
		return nil, errors.New("missing required environment variables for Gitlab audit source")
	}

	return NewGitlabAuditSource(gitlabProjectID, gitlabToken, gitlabHost)
}

func NewGitlabAuditSource(projectID, token, host string) (*GitlabAuditSource, error) {
	client, err := gitlab.NewClient(token, gitlab.WithBaseURL(host))
	if err != nil {
		return nil, err
	}

	return &GitlabAuditSource{
		projectID: projectID,
		Client:    client,
	}, nil
}

func (g *GitlabAuditSource) GetChangeOrigin(ctx context.Context) audit.ChangeOrigin {
	commitSHA := os.Getenv("CI_COMMIT_SHA")
	if commitSHA == "" {
		return audit.ChangeOrigin{}
	}

	// for a single commit, try to get the relevant MR info first to check if the commit is part of MR,
	// if not, then get the commit info alone
	var (
		author string
		meta   map[string]string
		err    error
	)
	author, meta, err = g.getAuditDataFromMR(ctx, commitSHA)
	if err != nil {
		author, meta, err = g.getAuditDataFromStandaloneCommit(ctx, commitSHA)
		if err != nil {
			return audit.ChangeOrigin{}
		}
	}

	return audit.ChangeOrigin{
		Author:   author,
		Source:   gitlabAuditSourceKey,
		Metadata: meta,
	}
}

func (g *GitlabAuditSource) getAuditDataFromMR(ctx context.Context, commitSHA string) (string, map[string]string, error) {
	mrs, _, err := g.Commits.ListMergeRequestsByCommit(g.projectID, commitSHA, gitlab.WithContext(ctx))
	if err != nil {
		return "", nil, err
	}
	if len(mrs) == 0 {
		return "", nil, errors.New("no merge request found for the commit")
	}

	// get the first MR associated with the commit
	mr := mrs[0]
	author := fmt.Sprintf("%s <%s>", mr.Author.Name, mr.Author.Username)
	meta := map[string]string{
		"merge_request_url": mr.WebURL,
		"commit_sha":        commitSHA,
		"approved_by":       fmt.Sprintf("%s <%s>", mr.MergedBy.Name, mr.MergedBy.Username),
	}

	return author, meta, nil
}

func (g *GitlabAuditSource) getAuditDataFromStandaloneCommit(ctx context.Context, commitSHA string) (string, map[string]string, error) {
	commit, _, err := g.Commits.GetCommit(g.projectID, commitSHA, gitlab.WithContext(ctx))
	if err != nil {
		return "", nil, err
	}

	author := fmt.Sprintf("%s <%s>", commit.AuthorName, commit.AuthorEmail)
	meta := map[string]string{
		"commit_sha": commit.ID,
		"merged_by":  author,
	}

	return author, meta, nil
}
