package maxcompute_test

import (
	"github.com/goto/optimus/ext/store/maxcompute"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMaxComputeClient(t *testing.T) {
	testCredJSON := `
{
  "access_id": "LNRJ5tH1XMSINW5J3TjYAvfX",
  "access_key": "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp",
  "endpoint": "http://service.ap-southeast-5.maxcompute.aliyun.com/api",
  "project_name": "test-maxcompute"
}
`

	t.Run("NewClient", func(t *testing.T) {
		t.Run("returns error when invalid creds", func(t *testing.T) {
			_, err := maxcompute.NewClient("")
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "failed to read account")
		})
		t.Run("returns success when create new client on valid creds", func(t *testing.T) {
			client, err := maxcompute.NewClient(testCredJSON)
			assert.Nil(t, err)
			assert.NotNil(t, client)
		})
	})
	t.Run("TableHandleFrom", func(t *testing.T) {
		t.Run("returns success when init the table handle", func(t *testing.T) {
			client, err := maxcompute.NewClient(testCredJSON)
			assert.Nil(t, err)

			tableHandle := client.TableHandleFrom()
			assert.NotNil(t, tableHandle)
		})
	})
}

func TestClientProvider(t *testing.T) {
	clientProvider := maxcompute.NewClientProvider()
	testCredJSON := `
{
 "type": "service_account",
 "project_id": "test-bigquery",
 "private_key_id": "4192b",
 "private_key": "-----BEGIN PRIVATE KEY-----\njLpyglDekLC\n-----END PRIVATE KEY-----\n",
 "client_email": "test-service-account@test-bigquery.iam.gserviceaccount.com",
 "client_id": "1234567890",
 "auth_uri": "https://accounts.google.com/o/oauth2/auth",
 "token_uri": "https://oauth2.googleapis.com/token",
 "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
 "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/test-service-account%40test-bigquery.iam.gserviceaccount.com"
}
`
	t.Run("return error when client provider cannot create new client", func(t *testing.T) {
		_, err := clientProvider.Get("")
		assert.NotNil(t, err)
		assert.ErrorContains(t, err, "failed to read account")
	})
	t.Run("return success when client provider creates new client with json", func(t *testing.T) {
		client, err := clientProvider.Get(testCredJSON)
		assert.Nil(t, err)
		assert.NotNil(t, client)
	})
}
