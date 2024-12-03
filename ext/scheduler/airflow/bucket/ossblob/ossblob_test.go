package ossblob_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/gcerrors"

	"github.com/goto/optimus/ext/scheduler/airflow/bucket/ossblob"
)

func TestOSSReader(t *testing.T) {
	body := io.NopCloser(strings.NewReader("test content"))
	raw := &oss.RangeReader{}
	reader := ossblob.NewOSSReader(body, raw)

	t.Run("Read", func(t *testing.T) {
		buf := make([]byte, 4)
		n, err := reader.Read(buf)
		assert.NoError(t, err)
		assert.Equal(t, 4, n)
		assert.Equal(t, "test", string(buf))
	})

	t.Run("Close", func(t *testing.T) {
		err := reader.Close()
		assert.NoError(t, err)
	})

	t.Run("As", func(t *testing.T) {
		var rr *oss.RangeReader
		assert.True(t, reader.As(&rr))
		assert.Equal(t, raw, rr)
	})

	t.Run("Attributes", func(t *testing.T) {
		attrs := reader.Attributes()
		assert.NotNil(t, attrs)
	})
}

func TestOSSWriter(t *testing.T) {
	pr, pw := io.Pipe()
	req := oss.PutObjectRequest{}
	writer := ossblob.NewOSSWriter(req, pr, pw)

	t.Run("As", func(t *testing.T) {
		var reqPtr *oss.PutObjectRequest
		assert.True(t, writer.As(&reqPtr))
		assert.Equal(t, &req, reqPtr)
	})
}

func TestOSSBucket(t *testing.T) {
	client := &oss.Client{}
	bucket := "test-bucket"
	b := ossblob.NewOSSBucket(client, bucket)

	t.Run("As", func(t *testing.T) {
		var clientPtr *oss.Client
		assert.True(t, b.As(&clientPtr))
		assert.Equal(t, client, clientPtr)
	})

	t.Run("ErrorCode", func(t *testing.T) {
		tests := []struct {
			err      error
			expected gcerrors.ErrorCode
		}{
			{&oss.ServiceError{StatusCode: http.StatusNotFound}, gcerrors.NotFound},
			{&oss.ServiceError{StatusCode: http.StatusForbidden}, gcerrors.PermissionDenied},
			{&oss.ServiceError{StatusCode: http.StatusInternalServerError}, gcerrors.Internal},
			{&oss.ServiceError{StatusCode: http.StatusConflict}, gcerrors.AlreadyExists},
			{&oss.ServiceError{StatusCode: http.StatusBadRequest}, gcerrors.InvalidArgument},
			{errors.New("unknown error"), gcerrors.Internal},
		}

		for _, tt := range tests {
			assert.Equal(t, tt.expected, b.ErrorCode(tt.err))
		}
	})

	t.Run("ErrorAs", func(t *testing.T) {
		var target *oss.ServiceError
		err := &oss.ServiceError{}
		assert.True(t, b.ErrorAs(err, &target))
		assert.Equal(t, err, target)
	})

	t.Run("Close", func(t *testing.T) {
		assert.NoError(t, b.Close())
	})
}

func TestOpenBucket(t *testing.T) {
	ctx := context.Background()
	cfg := &oss.Config{}
	bucketName := "test-bucket"

	t.Run("openBucket with nil config", func(t *testing.T) {
		_, err := ossblob.OpenBucket(ctx, nil, bucketName)
		assert.Error(t, err)
	})

	t.Run("openBucket with nil credentials provider", func(t *testing.T) {
		cfg.CredentialsProvider = nil
		_, err := ossblob.OpenBucket(ctx, cfg, bucketName)
		assert.Error(t, err)
	})

	t.Run("openBucket with empty bucket name", func(t *testing.T) {
		_, err := ossblob.OpenBucket(ctx, cfg, "")
		assert.Error(t, err)
	})

	t.Run("openBucket with valid config", func(t *testing.T) {
		cfg.CredentialsProvider = &credentials.StaticCredentialsProvider{}
		b, err := ossblob.OpenBucket(ctx, cfg, bucketName)
		assert.NoError(t, err)
		assert.NotNil(t, b)
	})
}
