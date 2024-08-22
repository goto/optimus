package lark

import (
	"context"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
	"testing"
	"time"
)

func TestLark(t *testing.T) {

	projectName := "ss"
	namespaceName := "bb"
	jobName := scheduler.JobName("foo-job-spec")
	tnnt, _ := tenant.NewTenant(projectName, namespaceName)

	t.Run("should send message to user groups successfully", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var sendErrors []error

		eventValues, _ := structpb.NewStruct(map[string]interface{}{
			"task_id":   "some_task_name",
			"duration":  "2s",
			"log_url":   "http://localhost:8081/tree?dag_id=hello_1",
			"message":   "some failure",
			"exception": "this much data failed",
		})

		client := NewNotifier(
			ctx,
			time.Millisecond*500,
			func(err error) {
				sendErrors = append(sendErrors, err)
			},
			scheduler.LarkNotifyAttrs{
				AppId:             "cli_a6218c2786f9102f",
				AppSecret:         "GillccWhlzoQPXT6zHNPIelK6MUmuGJN",
				VerificationToken: "TNl7DSVmJl1K6v47ZuYnGf3iko5vPGRA",
			},
		)
		defer client.Close()
		err := client.Notify(context.Background(), scheduler.NotifyAttrs{
			Owner: "testEmail@gojek.com",
			JobEvent: &scheduler.Event{
				JobName: jobName,
				Tenant:  tnnt,
				Type:    scheduler.SLAMissEvent,
				Values:  eventValues.AsMap(),
			},
			Route:  "#cmp-iac-test",
			Secret: "test-token",
		})

		assert.Nil(t, err)
		cancel()
		assert.Nil(t, client.Close())
		assert.Nil(t, sendErrors)
	})
}
