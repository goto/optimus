package connection_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/goto/optimus/client/cmd/internal/connection"
	"github.com/goto/optimus/core/audit"
)

func TestAppendContextWithAudit(t *testing.T) {
	t.Run("attaches author and source headers when ChangeOrigin is in context", func(t *testing.T) {
		origin := audit.NewChangeOrigin("test@example.com", "gitlab", nil)
		ctx := audit.ToContext(context.Background(), origin)

		resultCtx := connection.AppendContextWithAudit(ctx)

		md, ok := metadata.FromOutgoingContext(resultCtx)
		assert.True(t, ok)
		assert.Equal(t, []string{"test@example.com"}, md.Get(audit.HeaderAuthor))
		assert.Equal(t, []string{"gitlab"}, md.Get(audit.HeaderSource))
		assert.Empty(t, md.Get(audit.HeaderMetadata))
	})

	t.Run("attaches metadata header when ChangeOrigin has metadata", func(t *testing.T) {
		meta := audit.ChangeMetadata{"commit_sha": "abc123", "merge_request_url": "http://test"}
		origin := audit.NewChangeOrigin("test@example.com", "cli", meta)
		ctx := audit.ToContext(context.Background(), origin)

		resultCtx := connection.AppendContextWithAudit(ctx)

		md, ok := metadata.FromOutgoingContext(resultCtx)
		assert.True(t, ok)

		metaVals := md.Get(audit.HeaderMetadata)
		assert.Len(t, metaVals, 1)

		var decoded audit.ChangeMetadata
		assert.NoError(t, json.Unmarshal([]byte(metaVals[0]), &decoded))
		assert.Equal(t, "abc123", decoded["commit_sha"])
		assert.Equal(t, "http://test", decoded["merge_request_url"])
	})

	t.Run("does not attach any headers when ChangeOrigin is absent", func(t *testing.T) {
		ctx := context.Background()

		resultCtx := connection.AppendContextWithAudit(ctx)

		_, ok := metadata.FromOutgoingContext(resultCtx)
		assert.False(t, ok)
	})

	t.Run("does not attach headers when author and source are both empty", func(t *testing.T) {
		origin := audit.NewChangeOrigin("", "", nil)
		ctx := audit.ToContext(context.Background(), origin)

		resultCtx := connection.AppendContextWithAudit(ctx)

		_, ok := metadata.FromOutgoingContext(resultCtx)
		assert.False(t, ok)
	})
}
