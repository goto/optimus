package audit

import "context"

type ChangeOrigin struct {
	Author   string
	Source   string
	Metadata map[string]string
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
