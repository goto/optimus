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

type AlertStatus string

const (
	httpChannelBufferSize = 100
	eventBatchInterval    = time.Second * 10
	httpTimeout           = time.Second * 10

	StatusPending AlertStatus = "PENDING"
	StatusSent    AlertStatus = "SENT"
	StatusFailed  AlertStatus = "FAILED"
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
	Project  string                 `json:"-"`
	LogTag   string                 `json:"-"`
	Data     map[string]interface{} `json:"data"`
	Template string                 `json:"template"`
	Labels   map[string]string      `json:"labels"`
	Endpoint string                 `json:"-"`
}

func (a *AlertPayload) HasDefaultChannelLabel() bool {
	if _, ok := a.Labels[DefaultChannelLabel]; ok {
		return true
	}
	return false
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
	alertsRepo  AlertsRepo

	eventBatchInterval time.Duration
}

type AlertsRepo interface {
	Insert(ctx context.Context, payload *AlertPayload) (uuid.UUID, error)
	UpdateStatus(ctx context.Context, recordID uuid.UUID, status AlertStatus, message string) error
}

func (a *AlertManager) relay(alert *AlertPayload) {
	if alert.Endpoint == "" {
		// Don't alert if alert manager is not configured in server config
		return
	}
	go func() {
		a.alertChan <- alert
		eventsReceived.WithLabelValues(alert.Project, alert.LogTag).Inc()
	}()
}

func (a *AlertManager) PrepareAndSendEvent(alertPayload *AlertPayload) error {
	eventID := uuid.New()
	payloadJSON, err := json.Marshal(alertPayload)
	if err != nil {
		return fmt.Errorf("[alert manager] %s unable to serialise request body err: %w", eventID, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpTimeout)
	defer cancel()
	endpoint := alertPayload.Endpoint

	a.logger.Debug(fmt.Sprintf("[alert manager] %s sending request to alert manager url:%s, body:%s", eventID, endpoint, payloadJSON))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(payloadJSON))
	if err != nil {
		return fmt.Errorf("[alert manager] %s unable to prepare request for Alert Manager err: %w", eventID, err)
	}
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("[alert manager] %s unable to send request to Alert Manager err: %w", eventID, err)
	}

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("[alert manager] %s unable to read response body err: %w", eventID, err)
	}
	a.logger.Debug(fmt.Sprintf("[alert manager] %s  response code:%s, resp:%s", eventID, res.Status, body))

	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("[alert manager] %s non 200 status code received status: %s", eventID, res.Status)
	}

	return res.Body.Close()
}

func (a *AlertManager) logEvent(ctx context.Context, e *AlertPayload) (uuid.UUID, error) {
	if e.HasDefaultChannelLabel() {
		return a.alertsRepo.Insert(ctx, e)
	}
	return uuid.Nil, nil
}

func (a *AlertManager) worker(ctx context.Context) {
	defer a.wg.Done()
	for {
		select {
		case e := <-a.alertChan:
			logID, err := a.logEvent(ctx, e)
			if err != nil {
				a.logger.Error("failed to log event", "error", err)
			}
			var alertDeliveryStatus AlertStatus
			err = a.PrepareAndSendEvent(e) // nolint:contextcheck
			if err != nil {
				eventWorkerSendErrCounter.WithLabelValues(e.Project, e.LogTag, err.Error()).Inc()
				eventDataBytes, _ := json.Marshal(e.Data)
				a.workerErrChan <- fmt.Errorf("alert worker: event_info: [ %s ], err: %w", string(eventDataBytes), err)
				alertDeliveryStatus = StatusFailed
			} else {
				successSentCounter.WithLabelValues(e.Project, e.LogTag).Inc()
				alertDeliveryStatus = StatusSent
			}
			if e.HasDefaultChannelLabel() {
				if err != nil {
					a.alertsRepo.UpdateStatus(ctx, logID, alertDeliveryStatus, err.Error())
				} else {
					a.alertsRepo.UpdateStatus(ctx, logID, alertDeliveryStatus, "")
				}
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

func New(ctx context.Context, logger log.Logger, host, endpoint, dashboard, dataConsole string, alertsRepo AlertsRepo) *AlertManager {
	logger.Info(fmt.Sprintf("alert-manager: Starting alert-manager worker with config: \n host: %s \n endpoint: %s \n dashboard: %s \n dataConsole: %s\n", host, endpoint, dashboard, dataConsole))
	if host == "" {
		logger.Info("alert-manager: host name not found in server config, Optimus can still send events to Alert manager using tenant config.")
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
		alertsRepo:         alertsRepo,
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
