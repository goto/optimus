package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

const (
	httpChannelBufferSize = 100
	eventBatchInterval    = time.Second * 10
	httpTimeout           = time.Second * 10
	radarTimeFormat       = "2006/01/02 15:04:05"

	failureAlertTemplate        = "optimus-job-failure"
	slaAlertTemplate            = "optimus-job-sla-miss"
	successNotificationTemplate = "optimus-job-success"
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

	httpRegex = regexp.MustCompile(`^(http|https)://`)
)

type AlertManager struct {
	io.Closer

	alertChan     chan *scheduler.AlertAttrs
	wg            sync.WaitGroup
	workerErrChan chan error
	logger        log.Logger

	host        string
	endpoint    string
	dashboard   string
	dataConsole string

	eventBatchInterval time.Duration
}

type AlertPayload struct {
	Data     map[string]string `json:"data"`
	Template string            `json:"template"`
	Labels   map[string]string `json:"labels"`
}

func (a *AlertManager) Relay(alert *scheduler.AlertAttrs) {
	if a.host == "" {
		// Don't alert if alert manager is not configured in server config
		return
	}
	go func() {
		a.alertChan <- alert
		eventQueueCounter.Inc()
	}()
}

func RelayEvent(e *scheduler.AlertAttrs, host, endpoint, dashboardURL, dataConsole string, logger log.Logger) error {
	var template string
	var templateContext map[string]string

	dashURL, _ := url.Parse(dashboardURL)
	q := dashURL.Query()
	q.Set("var-project", e.JobEvent.Tenant.ProjectName().String())
	q.Set("var-namespace", e.JobEvent.Tenant.NamespaceName().String())
	q.Set("var-job", e.JobEvent.JobName.String())
	q.Set("var-schedule_time", e.JobEvent.JobScheduledAt.Format(radarTimeFormat))
	dashURL.RawQuery = q.Encode()
	templateContext = map[string]string{
		"project":      e.JobEvent.Tenant.ProjectName().String(),
		"namespace":    e.JobEvent.Tenant.NamespaceName().String(),
		"job_name":     e.JobEvent.JobName.String(),
		"owner":        e.Owner,
		"scheduled_at": e.JobEvent.JobScheduledAt.Format(radarTimeFormat),
		"console_link": fmt.Sprintf("%s/%s/%s", dataConsole, "optimus", e.JobEvent.JobName),
		"dashboard":    dashURL.String(),
	}

	if httpRegex.MatchString(e.SchedulerHost) {
		templateContext["airflow_logs"] = fmt.Sprintf("%s/dags/%s/grid", e.SchedulerHost, e.JobEvent.JobName)
	}

	switch e.JobEvent.Type {
	case scheduler.JobFailureEvent:
		template = failureAlertTemplate
		templateContext["task_id"] = e.JobEvent.OperatorName
	case scheduler.SLAMissEvent:
		template = slaAlertTemplate
		templateContext["state"] = e.JobEvent.Status.String()
	case scheduler.JobSuccessEvent:
		template = successNotificationTemplate
		templateContext["state"] = e.JobEvent.Status.String()
	}

	payload := AlertPayload{
		Data:     templateContext,
		Template: template,
		Labels: map[string]string{
			"identifier": e.JobURN,
			"event_type": e.JobEvent.Type.String(),
		},
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout) // nolint:contextcheck
	defer cancel()
	reqID, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	logger.Debug(fmt.Sprintf("sending request to alert manager url:%s, body:%s, reqID: %s", host+endpoint, payloadJSON, reqID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, host+endpoint, bytes.NewBuffer(payloadJSON))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	logger.Debug(fmt.Sprintf("alert manager response code:%s, resp:%s, reqID: %s", res.Status, body, reqID))

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("non 200 status code received status: %s", res.Status)
	}

	return res.Body.Close()
}

func (a *AlertManager) worker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case e := <-a.alertChan:
			err := RelayEvent(e, a.host, a.endpoint, a.dashboard, a.dataConsole, a.logger) // nolint:contextcheck
			if err != nil {
				a.workerErrChan <- fmt.Errorf("alert worker: %w", err)
				eventWorkerSendErrCounter.Inc()
			} else {
				eventWorkerSendCounter.Inc()
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

func New(ctx context.Context, logger log.Logger, host, endpoint, dashboard, dataConsole string) *AlertManager {
	logger.Info(fmt.Sprintf("alert-manager: Starting alert-manager worker with config: \n host: %s \n endpoint: %s \n dashboard: %s \n dataConsole: %s\n", host, endpoint, dashboard, dataConsole))
	if host == "" {
		logger.Info("alert-manager: host name not found in config, Optimus can not send events to Alert manager.")
		return &AlertManager{}
	}

	this := &AlertManager{
		alertChan:     make(chan *scheduler.AlertAttrs, httpChannelBufferSize),
		workerErrChan: make(chan error),
		wg:            sync.WaitGroup{},
		host:          host,
		logger:        logger,

		endpoint:           endpoint,
		dashboard:          dashboard,
		dataConsole:        dataConsole,
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			this.logger.Error("alert-manager : " + err.Error())
			eventWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.worker(ctx)
	return this
}
