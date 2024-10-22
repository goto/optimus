package ossblob

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"gocloud.dev/blob"
	"gocloud.dev/blob/driver"
	"gocloud.dev/gcerrors"
)

type ossBucket struct {
	client *oss.Client
	bucket string
}

func (b *ossBucket) As(i interface{}) bool {
	p, ok := i.(**oss.Client)
	if !ok {
		return false
	}
	*p = b.client
	return true
}

func (b *ossBucket) ErrorCode(err error) gcerrors.ErrorCode {
	if ossErr, ok := err.(*oss.ServiceError); ok {
		switch ossErr.StatusCode {
		case http.StatusNotFound:
			return gcerrors.NotFound
		case http.StatusForbidden:
			return gcerrors.PermissionDenied
		case http.StatusInternalServerError:
			return gcerrors.Internal
		case http.StatusConflict:
			return gcerrors.AlreadyExists
		case http.StatusBadRequest:
			return gcerrors.InvalidArgument
		}
	}
	return gcerrors.Internal
}

func (b *ossBucket) ErrorAs(err error, target interface{}) bool {
	switch ossErr := err.(type) {
	case *oss.ServiceError:
		if p, ok := target.(**oss.ServiceError); ok {
			*p = ossErr
			return true
		}
	case *oss.ClientError:
		if p, ok := target.(**oss.ClientError); ok {
			*p = ossErr
			return true
		}
	case *oss.CanceledError:
		if p, ok := target.(**oss.CanceledError); ok {
			*p = ossErr
			return true
		}
	case *oss.SerializationError:
		if p, ok := target.(**oss.SerializationError); ok {
			*p = ossErr
			return true
		}
	}
	return false
}

func (b *ossBucket) ListPaged(ctx context.Context, opts *driver.ListOptions) (*driver.ListPage, error) {
	req := &oss.ListObjectsV2Request{
		Bucket:    &b.bucket,
		Prefix:    &opts.Prefix,
		Delimiter: &opts.Delimiter,
		MaxKeys:   int32(opts.PageSize),
	}
	if req.MaxKeys == 0 {
		req.MaxKeys = 1000
	}

	paginator := b.client.NewListObjectsV2Paginator(req)
	if !paginator.HasNext() {
		return &driver.ListPage{}, nil
	}

	result, err := paginator.NextPage(ctx)
	if err != nil {
		return nil, err
	}

	page := &driver.ListPage{
		Objects: make([]*driver.ListObject, len(result.Contents)),
	}
	for i, obj := range result.Contents {
		asFunc := func(i interface{}) bool {
			p, ok := i.(*oss.ObjectProperties)
			if !ok {
				return false
			}
			*p = obj
			return true
		}
		page.Objects[i] = &driver.ListObject{
			Key:     safeGet(obj.Key),
			ModTime: safeGet(obj.LastModified),
			Size:    obj.Size,
			MD5:     []byte(safeGet(obj.ETag)),
			AsFunc:  asFunc,
		}
	}

	if result.NextContinuationToken != nil {
		page.NextPageToken = []byte(safeGet(result.NextContinuationToken))
	}

	return page, nil
}

func (b *ossBucket) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *driver.ReaderOptions) (driver.Reader, error) {
	request := oss.GetObjectRequest{
		Bucket: &b.bucket,
		Key:    &key,
	}

	getFn := func(ctx context.Context, httpRange oss.HTTPRange) (*oss.ReaderRangeGetOutput, error) {
		request.Range = nil
		rangeStr := httpRange.FormatHTTPRange()
		request.RangeBehavior = nil
		if rangeStr != nil {
			request.Range = rangeStr
			request.RangeBehavior = oss.Ptr("standard")
		}

		result, err := b.client.GetObject(ctx, &request)
		if err != nil {
			return nil, err
		}

		return &oss.ReaderRangeGetOutput{
			Body:          result.Body,
			ETag:          result.ETag,
			ContentLength: result.ContentLength,
			ContentRange:  result.ContentRange,
		}, nil
	}

	var emptyEtag string
	rangeReader, err := oss.NewRangeReader(ctx, getFn, &oss.HTTPRange{Offset: offset, Count: length}, emptyEtag)
	if err != nil {
		return nil, err
	}

	return &ossReader{
		body:  rangeReader,
		attrs: driver.ReaderAttributes{},
		raw:   rangeReader,
	}, nil
}

type ossReader struct {
	body  io.ReadCloser
	attrs driver.ReaderAttributes
	raw   *oss.RangeReader
}

func (r *ossReader) Read(p []byte) (int, error) {
	return r.body.Read(p)
}

func (r *ossReader) Close() error {
	return r.body.Close()
}

func (r *ossReader) As(i interface{}) bool {
	p, ok := i.(**oss.RangeReader)
	if !ok {
		return false
	}
	*p = r.raw
	return true
}

func (r *ossReader) Attributes() *driver.ReaderAttributes {
	return &r.attrs
}

// NewTypedWriter implements driver.NewTypedWriter.
func (b *ossBucket) NewTypedWriter(ctx context.Context, key string, contentType string, opts *driver.WriterOptions) (driver.Writer, error) {
	// Implementation for creating a new typed writer
	return nil, nil
}

// Delete implements driver.Delete.
func (b *ossBucket) Delete(ctx context.Context, key string) error {
	_, err := b.client.DeleteObject(ctx, &oss.DeleteObjectRequest{
		Bucket: &b.bucket,
		Key:    &key,
	})

	return err
}

// Attributes implements driver.Attributes.
func (b *ossBucket) Attributes(ctx context.Context, key string) (*driver.Attributes, error) {
	objMeta, err := b.client.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: &b.bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}

	attrs := driver.Attributes{
		CacheControl:       safeGet(objMeta.CacheControl),
		ContentDisposition: safeGet(objMeta.ContentDisposition),
		ContentEncoding:    safeGet(objMeta.ContentEncoding),
		Metadata:           objMeta.Metadata,
		Size:               objMeta.ContentLength,
		ModTime:            safeGet(objMeta.LastModified),
		MD5:                []byte(safeGet(objMeta.ContentMD5)),
		ETag:               safeGet(objMeta.ETag),
		AsFunc: func(i interface{}) bool {
			p, ok := i.(**oss.HeadObjectResult)
			if !ok {
				return false
			}
			*p = objMeta
			return true
		},
	}

	return &attrs, nil
}

// Close implements driver.Close.
func (b *ossBucket) Close() error {
	return nil
}

// SignedURL implements driver.SignedURL
func (b *ossBucket) SignedURL(ctx context.Context, key string, opts *driver.SignedURLOptions) (string, error) {
	res, err := b.client.Presign(ctx, &oss.GetObjectRequest{
		Bucket: &b.bucket,
		Key:    &key,
	}, oss.PresignExpires(opts.Expiry))
	if err != nil {
		return "", err
	}

	return res.URL, nil
}

// Copy implements driver.Copy
func (b *ossBucket) Copy(ctx context.Context, dstKey, srcKey string, opts *driver.CopyOptions) error {
	copier := oss.NewCopier(b.client)
	_, err := copier.Copy(ctx, &oss.CopyObjectRequest{
		Bucket:       &b.bucket,
		SourceBucket: &b.bucket,
		Key:          &dstKey,
		SourceKey:    &srcKey,
	})

	return err
}

func openBucket(_ context.Context, cred credentials.CredentialsProvider, endpoint, bucketName string) (*ossBucket, error) {
	if cred == nil {
		return nil, errors.New("ossblob.openBucket: credentials are required")
	}
	if bucketName == "" {
		return nil, errors.New("ossblob.openBucket: bucketName is required")
	}

	// TODO provide more available options
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(cred).
		WithEndpoint(endpoint)

	client := oss.NewClient(cfg)

	return &ossBucket{client: client, bucket: bucketName}, nil
}

func OpenBucket(ctx context.Context, cred credentials.CredentialsProvider, endpoint, bucketName string) (*blob.Bucket, error) {
	drv, err := openBucket(ctx, cred, endpoint, bucketName)
	if err != nil {
		return nil, err
	}

	return blob.NewBucket(drv), nil
}

func safeGet[T any](obj *T) T {
	var zero T
	if obj == nil {
		return zero
	}
	return *obj
}
