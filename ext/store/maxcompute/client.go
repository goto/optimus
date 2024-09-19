package maxcompute

import (
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
	AccessId    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
}

func NewClient(svcAccount string) (*MaxComputeClient, error) {
	cred, err := collectMaxComputeCredential([]byte(svcAccount))
	if err != nil {
		return nil, errors.InternalError(store, "failed to read account", err)
	}

	aliAccount := account.NewAliyunAccount(cred.AccessId, cred.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, cred.Endpoint)
	odpsIns.SetDefaultProjectName(cred.ProjectName)

	return &MaxComputeClient{odpsIns}, nil
}

func (c *MaxComputeClient) TableHandleFrom() TableResourceHandle {
	t := c.Tables()
	return NewTableHandle(&t)
}

func collectMaxComputeCredential(jsonData []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(jsonData, &creds); err != nil {
		return nil, err
	}

	return &creds, nil
}
