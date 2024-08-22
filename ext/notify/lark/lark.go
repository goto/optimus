package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/goto/optimus/core/scheduler"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	DefaultEventBatchInterval = time.Second * 10
	MaxSLAEventsToProcess     = 6
)

var (
	notifierType     = "lark"
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

		}
	}

	if len(receiverIDs) == 0 {
		return fmt.Errorf("failed to find notification route %s", attr.Route)
	}
	fmt.Println("This is the current reciever id", receiverIDs[0])

	//Secret is the verification token we can use for the bot
	s.queueNotification(receiverIDs, attr.Secret, attr)
	return nil
}

func (s *Notifier) queueNotification(receiverIDs []string, oauthSecret string, attr scheduler.NotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//reciever ID is the name of the channel we have to later fetch the open Id
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

type TemplateVariable struct {
	CardTitle       string `json:"card_title"`
	JobName         string `json:"job_name"`
	ScheduledAt     string `json:"scheduled_at"`
	TaskID          string `json:"task_id"`
	TaskName        string `json:"task_name"`
	OwnerEmail      string `json:"owner_email"`
	Duration        string `json:"duration"`
	LogURL          string `json:"log_url"`
	FooterException string `json:"footer_exception"`
	FooterMessage   string `json:"footer_message"`
}

type Data struct {
	TemplateId       string           `json:"template_id"`
	TemplateVariable TemplateVariable `json:"template_variable"`
}

type Content struct {
	Type string `json:"type"`
	Data Data   `json:"data"`
}

// todo: add it in the handler
const SLABreachedTemplateID string = "ctp_AA0LV7jVKCDK"
const JobFailureTemplateID string = "ctp_AA0zf7o45Ppp"

func buildMessageBlocks(events []event, workerErrChan chan error) string {

	data := Data{}
	content := Content{
		Type: "template",
		Data: data,
	}

	for _, evt := range events {
		jobName := evt.meta.JobName
		owner := evt.owner

		content.Data.TemplateVariable.JobName = jobName.String()
		content.Data.TemplateVariable.OwnerEmail = owner
		projectName := evt.meta.Tenant.ProjectName().String()
		namespaceName := evt.meta.Tenant.NamespaceName().String()
		if evt.meta.Type.IsOfType(scheduler.EventCategorySLAMiss) {
			content.Data.TemplateId = SLABreachedTemplateID
			content.Data.TemplateVariable.CardTitle = fmt.Sprintf("[Job] SLA Breached | %s/%s", projectName, namespaceName)
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
						//Task Name
						content.Data.TemplateVariable.TaskName = slaStr
					}
					// skip further SLA events
					if slaIdx > MaxSLAEventsToProcess {
						break
					}
				}
			}
		} else if evt.meta.Type.IsOfType(scheduler.EventCategoryJobFailure) {
			content.Data.TemplateId = JobFailureTemplateID
			content.Data.TemplateVariable.CardTitle = fmt.Sprintf("[Job] Failure | %s/%s", projectName, namespaceName)
			if scheduledAt, ok := evt.meta.Values["scheduled_at"]; ok && scheduledAt.(string) != "" {
				content.Data.TemplateVariable.ScheduledAt = scheduledAt.(string)
			}
			if duration, ok := evt.meta.Values["duration"]; ok && duration.(string) != "" {
				content.Data.TemplateVariable.Duration = duration.(string)
			}
			if taskID, ok := evt.meta.Values["task_id"]; ok && taskID.(string) != "" {
				content.Data.TemplateVariable.TaskID = taskID.(string)
			}
		} else {
			workerErrChan <- fmt.Errorf("worker_buildMessageBlocks: unknown event type: %v", evt.meta.Type)
			continue
		}

		if logURL, ok := evt.meta.Values["log_url"]; ok && logURL.(string) != "" {
			content.Data.TemplateVariable.LogURL = logURL.(string)
		}

		if exception, ok := evt.meta.Values["exception"]; ok && exception.(string) != "" {
			content.Data.TemplateVariable.FooterException = fmt.Sprintf("Exception:\n%s", exception.(string))
		}

		if message, ok := evt.meta.Values["message"]; ok && message.(string) != "" {
			content.Data.TemplateVariable.FooterMessage = fmt.Sprintf("Message:\n%s", message.(string))
		}
	}

	contentDataString, err := json.Marshal(content)
	if err != nil {
		return ""
	}

	fmt.Println(string(contentDataString))

	return string(contentDataString)
}

func (s *Notifier) Worker(ctx context.Context, larkAttrs scheduler.LarkNotifyAttrs) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		// iterate over all queued routeMsgBatch and
		for route, events := range s.routeMsgBatch {
			if len(events) == 0 {
				continue
			}

			larkClient := lark.NewClient(larkAttrs.AppId, larkAttrs.AppSecret)

			groupListReq := larkim.NewListChatReqBuilder().
				SortType(`ByCreateTimeAsc`).
				PageSize(20).
				Build()

			//get the app tenant token which need to be passed here
			bodyMap := make(map[string]string)
			bodyMap["app_id"] = larkAttrs.AppId
			bodyMap["app_secret"] = larkAttrs.AppSecret
			bodyJsonBytes, err := json.Marshal(bodyMap)

			if err != nil {
				fmt.Println(err)
				return
			}

			tenantToken, err := larkClient.Post(context.Background(),
				"/open-apis/auth/v3/tenant_access_token/internal",
				bodyJsonBytes,
				larkcore.AccessTokenTypeTenant)

			if err != nil {
				log.Fatalf("Error making request: %v", err)
			}
			tenantKeyString := fetchTenantKey(tenantToken.RawBody)
			listOfGroupsResponse, err := larkClient.Im.Chat.List(context.Background(), groupListReq, larkcore.WithTenantAccessToken(tenantKeyString))
			allTheGroupsBotIsPartOf := listOfGroupsResponse.Data.Items

			var groupChatId string

			for _, group := range allTheGroupsBotIsPartOf {
				cleanedString := strings.ReplaceAll(route.receiverID, "#", "")
				if *group.Name == cleanedString {
					groupChatId = *group.ChatId
				}
			}

			messageRequest := larkim.NewCreateMessageReqBuilder().
				ReceiveIdType(`chat_id`).
				Body(larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(groupChatId).
					MsgType(`interactive`).
					Content(buildMessageBlocks(events, s.workerErrChan)).
					Uuid(uuid.NewString()).
					Build()).
				Build()

			resp, err := larkClient.Im.Message.Create(context.Background(), messageRequest)
			if err != nil {
				fmt.Println(err)
				return
			}

			fmt.Println(resp.Code, resp.Msg, resp.RequestId())
		}
		s.mu.Unlock()
		larkWorkerBatchCounter.Inc()

		select {
		case <-ctx.Done():
			close(s.workerErrChan)
			return
		default:
			// send messages in batches of 5 secs
			time.Sleep(s.eventBatchInterval)
		}
	}
}

func (s *Notifier) Close() error { // nolint: unparam
	// drain batches
	s.wg.Wait()
	return nil
}

type Response struct {
	Code              int    `json:"code"`
	Msg               string `json:"msg"`
	TenantAccessToken string `json:"tenant_access_token"`
	Expire            int    `json:"expire"`
}

func fetchTenantKey(tenantTokenResponse []byte) string {
	var response Response
	err := json.Unmarshal(tenantTokenResponse, &response)
	if err != nil {
		log.Fatalf("Error unmarshalling response: %v", err)
	}
	return response.TenantAccessToken
}

func NewNotifier(ctx context.Context, eventBatchInterval time.Duration, errHandler func(error), larkAttrs scheduler.LarkNotifyAttrs) *Notifier {
	this := &Notifier{
		routeMsgBatch:      map[route][]event{},
		workerErrChan:      make(chan error),
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
			larkWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx, larkAttrs)
	return this
}
