package pagerduty // nolint:testpackage

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
)

func TestBuildPayloadCustomDetails(t *testing.T) {
	tnnt, err := tenant.NewTenant("proj", "ns")
	assert.NoError(t, err)

	evt := Event{
		owner: "test-owner",
		meta: &scheduler.Event{
			JobName:      "foo-job",
			Tenant:       tnnt,
			Type:         scheduler.JobFailureEvent,
			OperatorName: "bq2bq",
			Values: map[string]any{
				"log_url": "http://airflow/logs",
			},
		},
	}

	raw, err := buildPayloadCustomDetails(evt)
	assert.NoError(t, err)

	var details customDetails
	assert.NoError(t, json.Unmarshal([]byte(raw), &details))
	assert.Equal(t, "bq2bq", details.TaskID)
	assert.Equal(t, "test-owner", details.Owner)
	assert.Equal(t, "ns", details.Namespace)
	assert.Equal(t, "http://airflow/logs", details.LogURL)
}
