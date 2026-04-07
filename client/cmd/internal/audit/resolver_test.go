package audit_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	cmdaudit "github.com/goto/optimus/client/cmd/internal/audit"
)

func TestResolve(t *testing.T) {
	t.Run("returns default origin when source is unknown", func(t *testing.T) {
		origin := cmdaudit.Resolve(context.Background(), "unknown-source")

		assert.Equal(t, "unknown", origin.Author)
		assert.Equal(t, "optimus_cli", origin.Source)
		assert.Empty(t, origin.Metadata)
	})

	t.Run("returns default origin when source is gitlab but env vars are missing", func(t *testing.T) {
		t.Setenv("CI_JOB_TOKEN", "")
		t.Setenv("CI_SERVER_URL", "")
		t.Setenv("CI_PROJECT_ID", "")

		origin := cmdaudit.Resolve(context.Background(), "gitlab")

		assert.Equal(t, "unknown", origin.Author)
		assert.Equal(t, "optimus_cli", origin.Source)
	})

	t.Run("returns default origin when source is empty string", func(t *testing.T) {
		origin := cmdaudit.Resolve(context.Background(), "")

		assert.Equal(t, "unknown", origin.Author)
		assert.Equal(t, "optimus_cli", origin.Source)
	})
}
