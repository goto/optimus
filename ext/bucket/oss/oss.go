package bucket

import (
	"encoding/json"
	"errors"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

type OSSCredentials struct {
	AccessID      string `json:"access_key_id"`
	AccessKey     string `json:"access_key_secret"`
	Endpoint      string `json:"endpoint"`
	ProjectName   string `json:"project_name"`
	Region        string `json:"region"`
	SecurityToken string `json:"security_token"`
}

func NewOssClient(creds string) (*oss.Client, error) {
	cred, err := toOSSCredentials(creds)
	if err != nil {
		return nil, err
	}

	credProvider := credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey, cred.SecurityToken)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credProvider).
		WithEndpoint(cred.Endpoint).
		WithRegion(cred.Region)

	if cfg.CredentialsProvider == nil {
		return nil, errors.New("OSS: credentials provider is required")
	}

	return oss.NewClient(cfg), nil
}

func toOSSCredentials(creds string) (OSSCredentials, error) {
	var cred OSSCredentials
	if err := json.Unmarshal([]byte(creds), &cred); err != nil {
		return OSSCredentials{}, err
	}

	return cred, nil
}
