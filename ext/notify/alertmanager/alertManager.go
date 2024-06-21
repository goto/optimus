package alertmanager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	httpChannelBufferSize = 100
	eventBatchInterval    = time.Second * 10
	httpTimeout           = time.Second * 10
)

const (
	MetricNotificationQueue         = "notification_received_total"
	MetricNotificationWorkerSendErr = "notification_err_total"
	MetricNotificationSend          = "notification_sent_successfully"
)

var (
	notifierType   = "event"
	eventsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        MetricNotificationQueue,
		ConstLabels: map[string]string{"type": notifierType},
	}, []string{"project", "tag"})

	eventWorkerSendErrCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        MetricNotificationWorkerSendErr,
		ConstLabels: map[string]string{"type": notifierType},
	}, []string{"project", "tag", "msg"})

	successSentCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name:        MetricNotificationSend,
		ConstLabels: map[string]string{"type": notifierType},
	}, []string{"project", "tag"})
)

type AlertPayload struct {
	Project  string            `json:"-"`
	LogTag   string            `json:"-"`
	Data     map[string]string `json:"data"`
	Template string            `json:"template"`
	Labels   map[string]string `json:"labels"`
}

type AlertManager struct {
	io.Closer

	alertChan     chan *AlertPayload
	wg            sync.WaitGroup
	workerErrChan chan error
	logger        log.Logger

	endpoint    string
	dashboard   string
	dataConsole string

	eventBatchInterval time.Duration
}

func (a *AlertManager) relay(alert *AlertPayload) {
	if a.endpoint == "" {
		// Don't alert if alert manager is not configured in server config
		return
	}
	go func() {
		a.alertChan <- alert
		eventsReceived.WithLabelValues(alert.Project, alert.LogTag).Inc()
	}()
}

func (a *AlertManager) PrepareAndSendEvent(alertPayload *AlertPayload) error {
	payloadJSON, err := json.Marshal(alertPayload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	reqID := uuid.New()

	a.logger.Debug(fmt.Sprintf("sending request to alert manager url:%s, body:%s, reqID: %s", a.endpoint, payloadJSON, reqID))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.endpoint, bytes.NewBuffer(payloadJSON))
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
	a.logger.Debug(fmt.Sprintf("alert manager response code:%s, resp:%s, reqID: %s", res.Status, body, reqID))

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
			err := a.PrepareAndSendEvent(e) // nolint:contextcheck
			if err != nil {
				eventWorkerSendErrCounter.WithLabelValues(e.Project, e.LogTag, err.Error()).Inc()
				a.workerErrChan <- fmt.Errorf("alert worker: %w", err)
			} else {
				successSentCounter.WithLabelValues().Inc()
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
		alertChan:          make(chan *AlertPayload, httpChannelBufferSize),
		workerErrChan:      make(chan error),
		wg:                 sync.WaitGroup{},
		logger:             logger,
		endpoint:           host + endpoint,
		eventBatchInterval: eventBatchInterval,
		dashboard:          dashboard,
		dataConsole:        dataConsole,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			this.logger.Error("alert-manager : " + err.Error())
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.worker(ctx)
	return this
}
