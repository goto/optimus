package audit

import "context"

const (
	HeaderAuthor   = "x-author"
	HeaderSource   = "x-source"
	HeaderMetadata = "x-metadata"
)

type ChangeOrigin struct {
	Author   string
	Source   string
	Metadata ChangeMetadata
}

type ChangeMetadata map[string]string

func NewChangeOrigin(author, source string, metadata ChangeMetadata) ChangeOrigin {
	return ChangeOrigin{
		Author:   author,
		Source:   source,
		Metadata: metadata,
	}
}

type contextKey struct{}

func ToContext(ctx context.Context, origin ChangeOrigin) context.Context {
	return context.WithValue(ctx, contextKey{}, origin)
}

func FromContext(ctx context.Context) ChangeOrigin {
	if v, ok := ctx.Value(contextKey{}).(ChangeOrigin); ok {
		return v
	}
	return ChangeOrigin{}
}
