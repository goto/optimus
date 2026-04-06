package server

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/goto/optimus/core/audit"
)

const (
	defaultAuditAuthor = "unknown"
	defaultAuditSource = "unknown"
)

func auditUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx = extractAuditHeaders(ctx)
		return handler(ctx, req)
	}
}

func auditStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := extractAuditHeaders(ss.Context())
		return handler(srv, &wrappedServerStream{ServerStream: ss, ctx: ctx})
	}
}

func extractAuditHeaders(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	origin := audit.ChangeOrigin{
		Source: defaultAuditSource,
		Author: defaultAuditAuthor,
	}

	if vals := md.Get(audit.HeaderAuthor); len(vals) > 0 {
		origin.Author = vals[0]
	}
	if vals := md.Get(audit.HeaderSource); len(vals) > 0 {
		origin.Source = vals[0]
	}
	if vals := md.Get(audit.HeaderMetadata); len(vals) > 0 {
		var m audit.ChangeMetadata
		if err := json.Unmarshal([]byte(vals[0]), &m); err == nil {
			origin.Metadata = m
		}
	}

	return audit.ToContext(ctx, origin)
}

// nolint:containedctx
type wrappedServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedServerStream) Context() context.Context {
	return w.ctx
}
