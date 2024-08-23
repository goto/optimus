package lark

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
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
	appID      string
	appSecret  string
}

type event struct {
	owner string
	meta  *scheduler.Event
}

type Notifier struct {
	io.Closer

	routeMsgBatch map[route][]event // channelID -> [][][][][]
	wg            sync.WaitGroup
	mu            sync.Mutex
	workerErrChan chan error

	eventBatchInterval    time.Duration
	SLABreachedTemplateID string
	JobFailureTemplateID  string
}

func (s *Notifier) Notify(_ context.Context, attr scheduler.LarkNotifyAttrs) error { //nolint: gocritic
	var receiverIDs []string
	// channel this will have channel names in the array which we need to use when we need to fetch the open id
	if strings.HasPrefix(attr.Route, "#") {
		receiverIDs = append(receiverIDs, attr.Route)
	}

	// todo: we can add the changes for groups
	if len(receiverIDs) == 0 {
		return fmt.Errorf("failed to find notification route %s", attr.Route)
	}

	s.queueNotification(receiverIDs, attr)
	return nil
}

func (s *Notifier) queueNotification(receiverIDs []string, attr scheduler.LarkNotifyAttrs) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, receiverID := range receiverIDs {
		rt := route{
			receiverID: receiverID,
			appID:      attr.AppID,
			appSecret:  attr.AppSecret,
		}
		if _, ok := s.routeMsgBatch[rt]; !ok {
			s.routeMsgBatch[rt] = []event{}
		}

		evt := event{
			owner: attr.Owner,
			meta:  attr.JobEvent,
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
	TemplateID       string           `json:"template_id"`
	TemplateVariable TemplateVariable `json:"template_variable"`
}

type Content struct {
	Type string `json:"type"`
	Data Data   `json:"data"`
}

func (s *Notifier) buildMessageBlocks(events []event, workerErrChan chan error) string {
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
			content.Data.TemplateID = s.SLABreachedTemplateID
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

						content.Data.TemplateVariable.TaskName = slaStr
					}
					// skip further SLA events
					if slaIdx > MaxSLAEventsToProcess {
						break
					}
				}
			}
		} else if evt.meta.Type.IsOfType(scheduler.EventCategoryJobFailure) {
			content.Data.TemplateID = s.JobFailureTemplateID
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

	return string(contentDataString)
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		// iterate over all queued routeMsgBatch and
		for ro, events := range s.routeMsgBatch {
			if len(events) == 0 {
				continue
			}

			larkClient := lark.NewClient(ro.appID, ro.appSecret)

			const pageSize = 20
			groupListReq := larkim.NewListChatReqBuilder().
				SortType(`ByCreateTimeAsc`).
				PageSize(pageSize).
				Build()

			// todo: get the app tenant token which need to be passed here // create a new function for this to fetch tenant key
			bodyMap := make(map[string]string)
			bodyMap["app_id"] = ro.appID
			bodyMap["app_secret"] = ro.appSecret
			bodyJSONBytes, err := json.Marshal(bodyMap)
			if err != nil {
				log.Println(err)
				return
			}

			tenantTokenForGroupInfo := fetchTenantToken(ctx, larkClient, bodyJSONBytes)

			groupChatID := fetchGroupChatID(ctx, larkClient, tenantTokenForGroupInfo, groupListReq, ro)

			messageRequest := larkim.NewCreateMessageReqBuilder().
				ReceiveIdType(`chat_id`).
				Body(larkim.NewCreateMessageReqBodyBuilder().
					ReceiveId(groupChatID).
					MsgType(`interactive`).
					Content(s.buildMessageBlocks(events, s.workerErrChan)).
					Uuid(uuid.NewString()).
					Build()).
				Build()

			resp, err := larkClient.Im.Message.Create(ctx, messageRequest)
			if err != nil {
				log.Println(err)
				return
			}

			log.Println(resp.Code, resp.Msg, resp.RequestId())
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

func fetchGroupChatID(ctx context.Context, larkClient *lark.Client, tenantTokenForGroupInfo string, groupListReq *larkim.ListChatReq, ro route) string {
	listOfGroupsResponse, err := larkClient.Im.Chat.List(ctx, groupListReq, larkcore.WithTenantAccessToken(tenantTokenForGroupInfo))
	if err != nil {
		log.Println(err)
		return ""
	}
	allTheGroupsBotIsPartOf := listOfGroupsResponse.Data.Items

	var groupChatID string
	for _, group := range allTheGroupsBotIsPartOf {
		cleanedString := strings.ReplaceAll(ro.receiverID, "#", "")
		if *group.Name == cleanedString {
			groupChatID = *group.ChatId
		}
	}

	return groupChatID
}

func fetchTenantToken(ctx context.Context, larkClient *lark.Client, bodyJSONBytes []byte) string {
	tenantToken, err := larkClient.Post(ctx,
		"/open-apis/auth/v3/tenant_access_token/internal",
		bodyJSONBytes,
		larkcore.AccessTokenTypeTenant)
	if err != nil {
		log.Fatalf("Error making request: %v", err)
	}

	return fetchTenantKey(tenantToken.RawBody)
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

func NewNotifier(ctx context.Context, eventBatchInterval time.Duration, errHandler func(error), slaMissTemplate, failureTemplate string) *Notifier {
	this := &Notifier{
		routeMsgBatch:         map[route][]event{},
		workerErrChan:         make(chan error),
		eventBatchInterval:    eventBatchInterval,
		SLABreachedTemplateID: slaMissTemplate,
		JobFailureTemplateID:  failureTemplate,
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
	go this.Worker(ctx)
	return this
}
