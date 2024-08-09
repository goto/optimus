package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/goto/optimus/core/scheduler"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"strings"
	"sync"
	"time"
)

const (
	DefaultEventBatchInterval = time.Second * 10
	MaxSLAEventsToProcess     = 6
)

var (
	notifierType     = "slack"
	larkQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationQueue,
		ConstLabels: map[string]string{"type": notifierType},
	})
	larkWorkerBatchCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationWorkerBatch,
		ConstLabels: map[string]string{"type": notifierType},
	})
	larkWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationWorkerSendErr,
		ConstLabels: map[string]string{"type": notifierType},
	})
)

type route struct {
	receiverID string
	authToken  string
}

type event struct {
	authToken string
	owner     string
	meta      *scheduler.Event
}

type Notifier struct {
	io.Closer

	routeMsgBatch map[route][]event // channelID -> [][][][][]
	wg            sync.WaitGroup
	mu            sync.Mutex
	workerErrChan chan error

	eventBatchInterval time.Duration
}

func (s *Notifier) Notify(ctx context.Context, attr scheduler.NotifyAttrs) error {
	//need to add this to the Notify Attributes

	var receiverIDs []string

	// channel this will have channel names in the array which we need to use when we need to fetch the open id
	if strings.HasPrefix(attr.Route, "#") {
		receiverIDs = append(receiverIDs, attr.Route)
	}
	if strings.Contains(attr.Route, "@") {
		if strings.HasPrefix(attr.Route, "@") {
			// user group
			//todo: User groups are not supported in Lark will add the functionality once they are added
			//Confirm the user logic and than write the code

		}
	}

	if len(receiverIDs) == 0 {
		return fmt.Errorf("failed to find notification route %s", attr.Route)
	}

	//Secret is the verification token we can use for the bot
	s.queueNotification(receiverIDs, attr.Secret, attr)

	//req := larkim.NewCreateMessageReqBuilder().
	//	ReceiveIdType(`chat_id`).
	//	Body(larkim.NewCreateMessageReqBodyBuilder().
	//		ReceiveId(`oc_91e7920d6301309e787518ebe4a417de`).
	//		MsgType(`interactive`).
	//		Content(`{"type": "template", "data": { "template_id": "ctp_AA0zf7o45Ppp", "template_variable": {"card_title": "Job Failure"} } }`).
	//		Uuid(`a0d69e20-1dd1-458b-k525-dfeca4015206`).
	//		Build()).
	//	Build()

	//resp, err := client.Im.Message.Create(context.Background(), req)
	//if err != nil {
	//}

	return nil
}

func (s *Notifier) queueNotification(receiverIDs []string, oauthSecret string, attr scheduler.NotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//reciever ID is the name of the channel we have tpo later fetch the open Id
	for _, receiverID := range receiverIDs {
		rt := route{
			receiverID: receiverID,
			authToken:  oauthSecret,
		}
		if _, ok := s.routeMsgBatch[rt]; !ok {
			s.routeMsgBatch[rt] = []event{}
		}

		evt := event{
			authToken: oauthSecret,
			owner:     attr.Owner,
			meta:      attr.JobEvent,
		}

		s.routeMsgBatch[rt] = append(s.routeMsgBatch[rt], evt)
		larkQueueCounter.Inc()
	}
}

type Data struct {
	TemplateId      string `json:"template_id"`
	CardTitle       string `json:"card_title"`
	JobName         string `json:"job_name"`
	ScheduledAt     string `json:"scheduled_at"`
	TaskID          string `json:"task_id"`
	OwnerEmail      string `json:"owner_email"`
	Duration        string `json:"duration"`
	LogURL          string `json:"log_url"`
	FooterException string `json:"footer_exception"`
	FooterMessage   string `json:"footer_message"`
}

func NewData() Data {
	return Data{
		TemplateId: "ctp_AA0zf7o45Ppp", // default value of the template
	}
}

type Content struct {
	Type string `json:"type"`
	Data Data  `json:"data"`
}

func buildMessageBlocks(events []event, workerErrChan chan error) string {

	data := NewData()
	content := Content{
		Type: "template",
		Data: data,
	}

	for _, evt := range events {
		jobName := evt.meta.JobName
		owner := evt.owner

		data.JobName = jobName.String()
		data.OwnerEmail = owner
		projectName := evt.meta.Tenant.ProjectName().String()
		namespaceName := evt.meta.Tenant.NamespaceName().String()
		if evt.meta.Type.IsOfType(scheduler.EventCategorySLAMiss) {
			data.CardTitle = fmt.Sprintf("[Job] SLA Breached | %s/%s", projectName, namespaceName)
			if slas, ok := evt.meta.Values["slas"]; ok {
				for slaIdx, sla := range slas.([]any) {
					slaFields := sla.(map[string]any)
					slaStr := ""
					if taskID, ok := slaFields["task_id"]; ok {
						slaStr += "\nTask: " + taskID.(string)
					}
					if scheduledAt, ok := slaFields["scheduled_at"]; ok {
						slaStr += "\nScheduled at: " + scheduledAt.(string)
					}
					if slaStr != "" {
						if slaIdx > MaxSLAEventsToProcess {
							slaStr += "\nToo many breaches. Truncating..."
						}

						//We need to add the Breached Item tag in the message card
						//fmt.Sprintf("*Breached item:*%s", slaStr))
					}
					// skip further SLA events
					if slaIdx > MaxSLAEventsToProcess {
						break
					}
				}
			}
		} else if evt.meta.Type.IsOfType(scheduler.EventCategoryJobFailure) {
			data.CardTitle = fmt.Sprintf("[Job] Failure | %s/%s", projectName, namespaceName)
			if scheduledAt, ok := evt.meta.Values["scheduled_at"]; ok && scheduledAt.(string) != "" {
				data.ScheduledAt = scheduledAt.(string)
			}
			if duration, ok := evt.meta.Values["duration"]; ok && duration.(string) != "" {
				data.Duration = duration.(string)
			}
			if taskID, ok := evt.meta.Values["task_id"]; ok && taskID.(string) != "" {
				data.TaskID = taskID.(string)
			}
		} else {
			workerErrChan <- fmt.Errorf("worker_buildMessageBlocks: unknown event type: %v", evt.meta.Type)
			continue
		}

		if logURL, ok := evt.meta.Values["log_url"]; ok && logURL.(string) != "" {
			data.LogURL = logURL.(string)
		}

		if exception, ok := evt.meta.Values["exception"]; ok && exception.(string) != "" {
			data.FooterException = fmt.Sprintf("Exception:\n%s", exception.(string))
		}

		if message, ok := evt.meta.Values["message"]; ok && message.(string) != "" {
			data.FooterMessage = fmt.Sprintf("Message:\n%s", message.(string))
		}
	}

	contentDataString, err := json.Marshal(content)
	if err != nil {
		return ""
	}

	return string(contentDataString)
}


func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		// iterate over all queued routeMsgBatch and
		for route, events := range s.routeMsgBatch {
			if len(events) == 0 {
				continue
			}

			req := larkim.NewCreateMessageReqBuilder().
				ReceiveIdType(`chat_id`).
				Body(larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(`oc_91e7920d6301309e787518ebe4a417de`).
					MsgType(`interactive`).
					Content(buildMessageBlocks(events, s.workerErrChan)).
					Uuid(uuid.NewString()).
					Build()).
				Build()

			client := lark.NewClient("","")

			resp, err := client.Im.Message.Create(context.Background(), req)
			if err != nil {
			}

	}
}
