package bucket

import (
	"context"
	"net/url"

	"go.opentelemetry.io/otel"
	"gocloud.dev/blob"

	"github.com/goto/optimus/core/tenant"
	oss "github.com/goto/optimus/ext/bucket/oss"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/optimus/ext/scheduler/airflow/bucket/ossblob"
)

func (f *Factory) GetOSSBucket(ctx context.Context, tnnt tenant.Tenant, parsedURL *url.URL) (airflow.Bucket, error) {
	spanCtx, span := otel.Tracer("airflow/bucketFactory").Start(ctx, "GetOSSBucket")
	defer span.End()

	cred, err := f.secretsGetter.Get(spanCtx, tnnt.ProjectName(), tnnt.NamespaceName().String(), tenant.SecretStorageKey)
	if err != nil {
		return nil, err
	}

	ossClient, err := oss.NewOssClient(cred.Value())
	if err != nil {
		return nil, err
	}

	driver := ossblob.NewOSSBucket(ossClient, parsedURL.Host)

	return blob.NewBucket(driver), nil
}
