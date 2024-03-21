package alertManager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

const (
	httpChannelBufferSize = 100
	eventBatchInterval    = time.Second * 10
	httpTimeout           = time.Second * 10
	radarTimeFormat       = "2006/01/02 15:04:05"
)

var (
	notifierType      = "event"
	eventQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationQueue,
		ConstLabels: map[string]string{"type": notifierType},
	})

	eventWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationWorkerSendErr,
		ConstLabels: map[string]string{"type": notifierType},
	})

	eventWorkerSendCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationSend,
		ConstLabels: map[string]string{"type": notifierType},
	})
)

type AlertManager struct {
	io.Closer

	alertChan     chan *scheduler.AlertAttrs
	wg            sync.WaitGroup
	workerErrChan chan error

	host      string
	endpoint  string
	dashboard string

	eventBatchInterval time.Duration
}

type alertData struct {
	EventType scheduler.JobEventType `json:"num_alerts_firing"`
	Status    scheduler.EventStatus  `json:"status"`

	Severity  string `json:"severity"`
	Title     string `json:"alert_name"`
	Summary   string `json:"summary"`
	Dashboard string `json:"dashboard"`
}

type alertPayload struct {
	Data     alertData         `json:"data"`
	Template string            `json:"template"`
	Labels   map[string]string `json:"labels"`
}

func (a *AlertManager) Relay(alert *scheduler.AlertAttrs) {
	go func() {
		a.alertChan <- alert
		eventQueueCounter.Inc()
	}()
}

func (a *AlertManager) relayEvent(e *scheduler.AlertAttrs) error {

	dashboardVar := map[string]string{
		"var-project":       e.JobEvent.Tenant.ProjectName().String(),
		"var-namespace":     e.JobEvent.Tenant.NamespaceName().String(),
		"var-job":           e.JobEvent.JobName.String(),
		"var-schedule_time": e.JobEvent.JobScheduledAt.Format(radarTimeFormat),
	}

	dashboardUrl, _ := url.Parse(a.dashboard)
	q := dashboardUrl.Query()
	for k, v := range dashboardVar {
		q.Set(k, v)
	}
	dashboardUrl.RawQuery = q.Encode()

	client := &http.Client{}

	var notificationMsg string
	switch e.JobEvent.Type {
	case scheduler.JobFailureEvent:
		notificationMsg = fmt.Sprintf("*[Job]* `%s` :alert:\n"+
			"*Project*\t\t:\t%s\t\t\t*Namespace*\t:\t%s\n"+
			"*Owner*\t\t:\t<%s>\t\t*Job*\t\t\t:\t`%s`\n"+
			"*Task ID*\t\t:\t%s\t\t\t*Scheduled At*:\t`%s`\n",
			e.JobEvent.Status, e.JobEvent.Tenant.ProjectName(), e.JobEvent.Tenant.NamespaceName(),
			e.Owner, e.JobEvent.JobName, e.JobEvent.JobScheduledAt.Format(time.RFC822), e.JobEvent.OperatorName)
	case scheduler.SLAMissEvent:
		notificationMsg = fmt.Sprintf("[Job] SLA MISS :alert:\n"+
			"*Project*\t\t:\t%s\t\t\t*Namespace*\t:\t%s\n"+
			"*Owner*\t\t:\t<%s>\t\t*Job*\t\t\t:\t`%s`\nPending Tasks:\n",
			e.JobEvent.Tenant.ProjectName(), e.JobEvent.Tenant.NamespaceName(),
			e.Owner, e.JobEvent.JobName)
		for _, object := range e.JobEvent.SLAObjectList {
			notificationMsg += fmt.Sprintf("Task: %s\n", object.JobName)
		}
	}

	payload := alertPayload{
		Data: alertData{
			EventType: e.JobEvent.Type,
			Title:     e.Title,
			Status:    e.Status,
			Severity:  "CRITICAL",
			Summary:   notificationMsg,
			Dashboard: dashboardUrl.String(),
		},
		Labels: map[string]string{
			"job_urn":    e.JobURN,
			"event_type": e.JobEvent.Type.String(),
			"team":       "optimus_showcase", //Todo: remove before merging, hardcoded for testing
			"severity":   "WARNING",
		},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout) // nolint:contextcheck
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.host+a.endpoint, bytes.NewBuffer(payloadJSON))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("non 200 status code received status: %s", res.Status)
	}
	eventWorkerSendCounter.Inc()

	return res.Body.Close()
}

func (a *AlertManager) Worker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case e := <-a.alertChan:
			err := a.relayEvent(e) // nolint:contextcheck
			if err != nil {
				a.workerErrChan <- fmt.Errorf("alert worker: %w", err)
			}
		case <-ctx.Done():
			close(a.workerErrChan)
			return
		default:
			// send messages in batches of 10 secs
			time.Sleep(a.eventBatchInterval)
		}
	}
}

func (a *AlertManager) Close() error { // nolint: unparam
	a.wg.Wait()
	return nil
}

func New(ctx context.Context, errHandler func(error), host, endpoint, dashboard string) *AlertManager {
	ch := make(chan *scheduler.AlertAttrs, httpChannelBufferSize)
	this := &AlertManager{
		alertChan:          ch,
		wg:                 sync.WaitGroup{},
		workerErrChan:      make(chan error),
		host:               host,
		endpoint:           endpoint,
		dashboard:          dashboard,
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
			eventWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
