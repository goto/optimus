package maxcompute_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/ext/store/maxcompute"
)

func TestMaxComputeClient(t *testing.T) {
	testCredJSON := `
{
  "access_id": "LNRJ5tH1XMSINW5J3TjYAvfX",
  "access_key": "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp",
  "endpoint": "http://service.ap-southeast-5.maxcompute.aliyun.com/api",
  "project_name": "proj"
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

			projectSchema, err := maxcompute.ProjectSchemaFrom("proj", "schema")
			assert.Nil(t, err)

			maskingPolicyHandle := client.TableMaskingPolicyHandleFrom(projectSchema, nil)
			assert.Nil(t, err)

			tableHandle := client.TableHandleFrom(projectSchema, maskingPolicyHandle)
			assert.NotNil(t, tableHandle)
		})
	})
	t.Run("ViewHandleFrom", func(t *testing.T) {
		t.Run("returns success when init the view handle", func(t *testing.T) {
			client, err := maxcompute.NewClient(testCredJSON)
			assert.Nil(t, err)

			projectSchema, err := maxcompute.ProjectSchemaFrom("proj", "schema")
			assert.Nil(t, err)

			viewHandle := client.ViewHandleFrom(projectSchema)
			assert.NotNil(t, viewHandle)
		})
	})

	t.Run("SchemaHandleFrom", func(t *testing.T) {
		t.Run("returns success when init the schema handle", func(t *testing.T) {
			client, err := maxcompute.NewClient(testCredJSON)
			assert.Nil(t, err)

			projectSchema, err := maxcompute.ProjectSchemaFrom("proj", "schema")
			assert.Nil(t, err)

			schemaHandle := client.SchemaHandleFrom(projectSchema)
			assert.NotNil(t, schemaHandle)
		})
	})

	t.Run("FunctionHandleFrom", func(t *testing.T) {
		t.Run("returns success when init the schema handle", func(t *testing.T) {
			client, err := maxcompute.NewClient(testCredJSON)
			assert.Nil(t, err)

			projectSchema, err := maxcompute.ProjectSchemaFrom("proj", "schema")
			assert.Nil(t, err)

			schemaHandle := client.FunctionHandleFrom(projectSchema)
			assert.NotNil(t, schemaHandle)
		})
	})
}

func TestClientProvider(t *testing.T) {
	clientProvider := maxcompute.NewClientProvider()
	testCredJSON := `
{
  "access_id": "LNRJ5tH1XMSINW5J3TjYAvfX",
  "access_key": "lAZBJhdkNbwVj3bej5BuhjwbdV0nSp",
  "endpoint": "http://service.ap-southeast-5.maxcompute.aliyun.com/api",
  "project_name": "proj"
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
