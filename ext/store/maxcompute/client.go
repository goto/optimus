package maxcompute

import (
	"context"
	"encoding/json"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"

	"github.com/goto/optimus/internal/errors"
)

type MaxComputeClientProvider struct{}

func NewClientProvider() *MaxComputeClientProvider {
	return &MaxComputeClientProvider{}
}

func (MaxComputeClientProvider) Get(account string) (Client, error) {
	return NewClient(account)
}

type MaxComputeClient struct {
	*odps.Odps
}

type maxComputeCredentials struct {
	AccessID    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
}

func NewClient(svcAccount string) (*MaxComputeClient, error) {
	cred, err := collectMaxComputeCredential([]byte(svcAccount))
	if err != nil {
		return nil, errors.InternalError(store, "failed to read account", err)
	}

	aliAccount := account.NewAliyunAccount(cred.AccessID, cred.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, cred.Endpoint)

	return &MaxComputeClient{odpsIns}, nil
}

func (c *MaxComputeClient) TableHandleFrom(projectSchema ProjectSchema) TableResourceHandle {
	c.SetDefaultProjectName(projectSchema.Project)
	c.SetCurrentSchemaName(projectSchema.Schema)
	s := c.Schemas()
	t := c.Tables()
	return NewTableHandle(c, s, t)
}

func (c *MaxComputeClient) ViewHandleFrom(projectSchema ProjectSchema) TableResourceHandle {
	c.SetDefaultProjectName(projectSchema.Project)
	c.SetCurrentSchemaName(projectSchema.Schema)
	s := c.Schemas()
	t := c.Tables()
	return NewViewHandle(c, s, t)
}

func (c *MaxComputeClient) GetDDLView(_ context.Context, table string) (string, error) {
	t := c.Odps.Table(table)
	if err := t.Load(); err != nil {
		return "", errors.InternalError(store, "failed to load table", err)
	}

	if t.Schema().IsVirtualView {
		return t.Schema().ViewText, nil
	}
	return "", nil
}

func collectMaxComputeCredential(jsonData []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(jsonData, &creds); err != nil {
		return nil, err
	}

	return &creds, nil
}
