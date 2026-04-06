package internal

import (
	"context"
	"os"

	"github.com/goto/optimus/client/cmd/internal/audit"
)

const (
	optimusChangeSourceEnvKey = "OPTIMUS_CHANGE_SOURCE"
)

// NewBaseContext creates a new base context to be used in Optimus CLI client
func NewBaseContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	changeSource := os.Getenv(optimusChangeSourceEnvKey)
	ctx = audit.ToContext(ctx, audit.Resolve(ctx, changeSource))

	return ctx
}
