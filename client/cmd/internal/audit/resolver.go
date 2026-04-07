package audit

import (
	"context"

	"github.com/goto/optimus/core/audit"
)

const (
	defaultClientChangeAuthor = "unknown"
	defaultClientChangeSource = "optimus_cli"
)

// Resolve will obtain the change origin information based on the provided source,
// if the source is not recognized or any error occurs during resolution, it will
// return a default change origin with "unknown" author and "optimus_cli" source.
func Resolve(ctx context.Context, source string) audit.ChangeOrigin {
	if source == gitlabAuditSourceKey {
		gitlabSource, err := NewGitlabAuditSourceFromEnv()
		if err == nil {
			return gitlabSource.GetChangeOrigin(ctx)
		}
	}

	// all fallbacks should use default
	return defaultClientChangeOrigin()
}

func defaultClientChangeOrigin() audit.ChangeOrigin {
	return audit.NewChangeOrigin(defaultClientChangeAuthor, defaultClientChangeSource, audit.ChangeMetadata{})
}

func ToContext(ctx context.Context, origin audit.ChangeOrigin) context.Context {
	return audit.ToContext(ctx, origin)
}
