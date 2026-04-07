package server_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/goto/optimus/core/audit"
	"github.com/goto/optimus/server"
)

func incomingCtx(pairs ...string) context.Context {
	md := metadata.Pairs(pairs...)
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestExtractAuditHeaders(t *testing.T) {
	t.Run("sets author and source from headers", func(t *testing.T) {
		ctx := incomingCtx(
			audit.HeaderAuthor, "alice@example.com",
			audit.HeaderSource, "gitlab-ci",
		)

		resultCtx := server.ExtractAuditHeaders(ctx)
		origin := audit.FromContext(resultCtx)

		assert.Equal(t, "alice@example.com", origin.Author)
		assert.Equal(t, "gitlab-ci", origin.Source)
		assert.Empty(t, origin.Metadata)
	})

	t.Run("parses metadata JSON from header", func(t *testing.T) {
		meta := audit.ChangeMetadata{"commit_sha": "abc123", "branch": "main"}
		metaJSON, _ := json.Marshal(meta)

		ctx := incomingCtx(
			audit.HeaderAuthor, "bob@example.com",
			audit.HeaderSource, "cli",
			audit.HeaderMetadata, string(metaJSON),
		)

		resultCtx := server.ExtractAuditHeaders(ctx)
		origin := audit.FromContext(resultCtx)

		assert.Equal(t, "abc123", origin.Metadata["commit_sha"])
		assert.Equal(t, "main", origin.Metadata["branch"])
	})

	t.Run("ignores malformed metadata JSON", func(t *testing.T) {
		ctx := incomingCtx(
			audit.HeaderAuthor, "carol@example.com",
			audit.HeaderMetadata, "not-valid-json",
		)

		resultCtx := server.ExtractAuditHeaders(ctx)
		origin := audit.FromContext(resultCtx)

		assert.Equal(t, "carol@example.com", origin.Author)
		assert.Empty(t, origin.Metadata)
	})

	t.Run("defaults to unknown when no headers present but incoming metadata exists", func(t *testing.T) {
		md := metadata.New(nil)
		ctx := metadata.NewIncomingContext(context.Background(), md)

		resultCtx := server.ExtractAuditHeaders(ctx)
		origin := audit.FromContext(resultCtx)

		assert.Equal(t, "unknown", origin.Author)
		assert.Equal(t, "unknown", origin.Source)
	})

	t.Run("returns ctx unchanged when no incoming gRPC metadata", func(t *testing.T) {
		ctx := context.Background()

		resultCtx := server.ExtractAuditHeaders(ctx)

		origin := audit.FromContext(resultCtx)
		assert.Empty(t, origin.Author)
	})
}
