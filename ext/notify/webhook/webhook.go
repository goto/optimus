package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

const (
	httpChannelBuffer         = 100
	DefaultEventBatchInterval = time.Second * 10
	httpWebhookTimeout        = time.Second * 10
)

var (
	notifierType        = "webhook"
	webhookQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationQueue,
		ConstLabels: map[string]string{"type": notifierType},
	})

	webhookWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationWorkerSendErr,
		ConstLabels: map[string]string{"type": notifierType},
	})

	webhookWorkerSendCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationSend,
		ConstLabels: map[string]string{"type": notifierType},
	})
)

type Notifier struct {
	io.Closer

	eventChan     chan event
	wg            sync.WaitGroup
	workerErrChan chan error

	eventBatchInterval time.Duration
}

type event struct {
	url     string
	headers map[string]string
	meta    *scheduler.Event
	jobMeta *scheduler.JobRunMeta
}

type webhookPayload struct {
	JobName     string            `json:"job_name"`
	Project     string            `json:"project"`
	Namespace   string            `json:"namespace"`
	Destination string            `json:"destination"`
	ScheduledAt string            `json:"scheduled_at"`
	Status      string            `json:"status"`
	JobLabel    map[string]string `json:"job_label,omitempty"`
}

func (s *Notifier) Trigger(attr scheduler.WebhookAttrs) {
	go func() {
		s.eventChan <- event{
			url:     attr.Route,
			meta:    attr.JobEvent,
			jobMeta: attr.Meta,
			headers: attr.Headers,
		}
		webhookQueueCounter.Inc()
	}()
}

func fireWebhook(e event) error {
	client := &http.Client{}
	payload := webhookPayload{
		JobName:     e.meta.JobName.String(),
		Project:     e.meta.Tenant.ProjectName().String(),
		Namespace:   e.meta.Tenant.NamespaceName().String(),
		Destination: e.jobMeta.DestinationURN,
		ScheduledAt: e.meta.JobScheduledAt.String(),
		Status:      e.meta.Status.String(),
		JobLabel:    e.jobMeta.Labels,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpWebhookTimeout) // nolint:contextcheck
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url, bytes.NewBuffer(payloadJSON))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	for name, value := range e.headers {
		req.Header.Add(name, value)
	}

	res, err := client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("non 200 status code received from endpoint:'%s', response status: %s", e.url, res.Status)
	}
	webhookWorkerSendCounter.Inc()

	return res.Body.Close()
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case e := <-s.eventChan:
			err := fireWebhook(e) // noLint:contextcheck
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w", err)
			}
		case <-ctx.Done():
			close(s.workerErrChan)
			return
		default:
			// send messages in batches of 10 secs
			time.Sleep(s.eventBatchInterval)
		}
	}
}

func (s *Notifier) Close() error { // nolint: unparam
	s.wg.Wait()
	return nil
}

func NewNotifier(ctx context.Context, eventBatchInterval time.Duration, errHandler func(error)) *Notifier {
	ch := make(chan event, httpChannelBuffer)
	this := &Notifier{
		eventChan:          ch,
		workerErrChan:      make(chan error),
		eventBatchInterval: eventBatchInterval,
	}

	this.wg.Add(1)
	go func() {
		for err := range this.workerErrChan {
			errHandler(err)
			webhookWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
