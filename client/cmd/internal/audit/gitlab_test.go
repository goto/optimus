package audit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmdaudit "github.com/goto/optimus/client/cmd/internal/audit"
)

func gitlabSource(t *testing.T, host string) *cmdaudit.GitlabAuditSource {
	t.Helper()
	src, err := cmdaudit.NewGitlabAuditSource("project-1", "fake-token", host)
	require.NoError(t, err)
	return src
}

func TestGitlabAuditSource_GetChangeOrigin(t *testing.T) {
	t.Run("returns author and MR metadata when commit is part of a merge request", func(t *testing.T) {
		commitSHA := "abc123def456"
		t.Setenv("CI_COMMIT_SHA", commitSHA)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			expected := fmt.Sprintf("/api/v4/projects/project-1/repository/commits/%s/merge_requests", commitSHA)
			if r.URL.Path == expected {
				resp := []map[string]interface{}{
					{
						"iid":           42,
						"web_url":       "https://gitlab.example.com/project/mr/42",
						"source_branch": "feature/my-feature",
						"author":        map[string]interface{}{"name": "Alice Developer"},
						"merged_by":     map[string]interface{}{"name": "Bob Reviewer"},
					},
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(resp)
				return
			}
			http.NotFound(w, r)
		}))
		defer srv.Close()

		origin := gitlabSource(t, srv.URL).GetChangeOrigin(context.Background())

		assert.Equal(t, "Alice Developer", origin.Author)
		assert.Equal(t, "gitlab", origin.Source)
		assert.Equal(t, "https://gitlab.example.com/project/mr/42", origin.Metadata["merge_request_url"])
		assert.Equal(t, commitSHA, origin.Metadata["commit_sha"])
		assert.Equal(t, "Bob Reviewer", origin.Metadata["approved_by"])
	})

	t.Run("falls back to standalone commit when no MR exists", func(t *testing.T) {
		commitSHA := "deadbeef1234"
		t.Setenv("CI_COMMIT_SHA", commitSHA)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mrPath := fmt.Sprintf("/api/v4/projects/project-1/repository/commits/%s/merge_requests", commitSHA)
			commitPath := fmt.Sprintf("/api/v4/projects/project-1/repository/commits/%s", commitSHA)

			switch r.URL.Path {
			case mrPath:
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]interface{}{})
			case commitPath:
				commit := map[string]interface{}{
					"id":          commitSHA,
					"author_name": "Carol Committer",
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(commit)
			default:
				http.NotFound(w, r)
			}
		}))
		defer srv.Close()

		origin := gitlabSource(t, srv.URL).GetChangeOrigin(context.Background())

		assert.Equal(t, "Carol Committer", origin.Author)
		assert.Equal(t, "gitlab", origin.Source)
		assert.Equal(t, commitSHA, origin.Metadata["commit_sha"])
	})

	t.Run("returns empty origin when CI_COMMIT_SHA is not set", func(t *testing.T) {
		t.Setenv("CI_COMMIT_SHA", "")

		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()

		origin := gitlabSource(t, srv.URL).GetChangeOrigin(context.Background())

		assert.Empty(t, origin.Author)
		assert.Empty(t, origin.Source)
	})

	t.Run("returns empty origin when GitLab API call fails", func(t *testing.T) {
		commitSHA := "failsha"
		t.Setenv("CI_COMMIT_SHA", commitSHA)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		}))
		defer srv.Close()

		origin := gitlabSource(t, srv.URL).GetChangeOrigin(context.Background())

		assert.Empty(t, origin.Author)
	})
}
