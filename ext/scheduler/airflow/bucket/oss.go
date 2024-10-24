package bucket

import (
	"context"
	"encoding/json"
	"net/url"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"github.com/goto/optimus/ext/scheduler/airflow/bucket/ossblob"
	"go.opentelemetry.io/otel"
)

type ossCredentials struct {
	AccessID      string `json:"access_key_id"`
	AccessKey     string `json:"access_key_secret"`
	Endpoint      string `json:"endpoint"`
	ProjectName   string `json:"project_name"`
	Region        string `json:"region"`
	SecurityToken string `json:"security_token"`
}

func (f *Factory) GetOSSBucket(ctx context.Context, tnnt tenant.Tenant, parsedURL *url.URL) (airflow.Bucket, error) {
	spanCtx, span := otel.Tracer("airflow/bucketFactory").Start(ctx, "GetOSSBucket")
	defer span.End()

	cred, err := f.getOSSCredentials(spanCtx, tnnt)
	if err != nil {
		return nil, err
	}

	credProvider := credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey, cred.SecurityToken)

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credProvider).
		WithEndpoint(cred.Endpoint).
		WithRegion(cred.Region)

	client, err := ossblob.OpenBucket(ctx, cfg, parsedURL.Host)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func (f *Factory) getOSSCredentials(ctx context.Context, tnnt tenant.Tenant) (ossCredentials, error) {
	// Get credentials from secret manager
	secret, err := f.secretsGetter.Get(ctx, tnnt.ProjectName(), tnnt.NamespaceName().String(), tenant.SecretStorageKey)
	if err != nil {
		return ossCredentials{}, err
	}

	var cred ossCredentials
	if err := json.Unmarshal([]byte(secret.Value()), &cred); err != nil {
		return ossCredentials{}, err
	}

	return cred, nil
}
