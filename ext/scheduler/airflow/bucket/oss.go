package bucket

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/optimus/ext/scheduler/airflow/bucket/ossblob"
	"go.opentelemetry.io/otel"
)

type ossCredentials struct {
	AccessID    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
	Region      string `json:"region"`
}

func (f *Factory) GetOSSBucket(ctx context.Context, tnnt tenant.Tenant, parsedURL *url.URL) (airflow.Bucket, error) {
	spanCtx, span := otel.Tracer("airflow/bucketFactory").Start(ctx, "GetOSSBucket")
	defer span.End()

	storageSecret, err := f.secretsGetter.Get(spanCtx, tnnt.ProjectName(), tnnt.NamespaceName().String(), tenant.SecretStorageKey)
	if err != nil {
		return nil, err
	}

	var cred ossCredentials
	if err := json.Unmarshal([]byte(storageSecret.Value()), &cred); err != nil {
		return nil, err
	}

	credProvider := credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey, "")

	client, err := ossblob.OpenBucket(ctx, credProvider, cred.Endpoint, cred.Region, parsedURL.Host)
	if err != nil {
		return nil, err
	}

	return client, nil
}
