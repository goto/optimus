package filesystem

import (
	"context"
	"net/url"

	"gocloud.dev/blob/fileblob"

	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
)

type SecretGetter interface {
	Get(ctx context.Context, projName tenant.ProjectName, namespaceName, name string) (*tenant.PlainTextSecret, error)
}

type ProjectGetter interface {
	Get(context.Context, tenant.ProjectName) (*tenant.Project, error)
}

type Factory struct {
	secretsGetter SecretGetter
	projectGetter ProjectGetter
}

func (f *Factory) New(ctx context.Context, tnnt tenant.Tenant) (SchedulerFS, error) {
	// TODO: implement here
	parsedURL := &url.URL{}
	switch parsedURL.Scheme {
	case "gs":
		return f.newGCSBucket(ctx, tnnt, parsedURL)
	case "file":
		return f.newFileBlob(ctx, parsedURL.Path, &fileblob.Options{})
	case "mem":
		return f.newMemBlob(ctx)
	}
	return nil, errors.InvalidArgument("airflow", "unsupported storage config "+parsedURL.String())
}

func (f *Factory) newGCSBucket(ctx context.Context, tnnt tenant.Tenant, parsedURL *url.URL) (SchedulerFS, error) {
	// TODO: move implementation of GetGCSBucket here
	return newGCSBucket(), nil
}

func (f *Factory) newFileBlob(ctx context.Context, path string, opts *fileblob.Options) (SchedulerFS, error) {
	// TODO: implement file blob here
	return newFileBlob(), nil
}

func (f *Factory) newMemBlob(ctx context.Context) (SchedulerFS, error) {
	// TODO: implement mem blob
	return newMemBlob(), nil
}
