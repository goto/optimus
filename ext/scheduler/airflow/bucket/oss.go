package bucket

import (
	"bytes"
	"context"
	"encoding/json"
	"net/url"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/ext/scheduler/airflow"
	"go.opentelemetry.io/otel"
	"gocloud.dev/blob"
)

type OSSBucket struct {
	client     *oss.Client
	bucketName string
}

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

	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey)).
		WithRegion(cred.Region)

	client := oss.NewClient(cfg)

	return &OSSBucket{client: client, bucketName: parsedURL.Host}, nil
}

func (b *OSSBucket) WriteAll(ctx context.Context, key string, p []byte, opts *blob.WriterOptions) error {
	buf := bytes.NewBuffer(p)
	request := oss.PutObjectRequest{
		Bucket: &b.bucketName,
		Key:    &key,
		Body:   buf,
	}

	if opts != nil {
		if opts.ContentType != "" {
			request.ContentType = &opts.ContentType
		}

		if opts.ContentDisposition != "" {
			request.ContentDisposition = &opts.ContentDisposition
		}

		if opts.ContentEncoding != "" {
			request.ContentEncoding = &opts.ContentEncoding
		}

		if opts.CacheControl != "" {
			request.CacheControl = &opts.CacheControl
		}

		if len(opts.ContentMD5) > 0 {
			contentMD5 := string(opts.ContentMD5)
			request.ContentMD5 = &contentMD5
		}

		if opts.Metadata != nil {
			request.Metadata = make(map[string]string)
			for k, v := range opts.Metadata {
				request.Metadata[k] = v
			}
		}
	}

	_, err := b.client.PutObject(ctx, &request)
	return err
}

func (b *OSSBucket) List(opts *blob.ListOptions) *blob.ListIterator {
	p := b.client.NewListObjectsV2Paginator(req)
}

func (b *OSSBucket) Delete(ctx context.Context, key string) error {
	bucket, err := b.client.Bucket(opts.BucketName)
	if err != nil {
		return err
	}
	return bucket.DeleteObject(ctx, key)
}

func (b *OSSBucket) Close() error {
	// No explicit close operation is needed for OSS client
	return nil
}
