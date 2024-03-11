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

func (s *Notifier) Notify(_ context.Context, attr scheduler.WebhookAttrs) error { //nolint:gocritic,unparam
	go func() {
		s.eventChan <- event{
			url:     attr.Route,
			meta:    attr.JobEvent,
			jobMeta: attr.Meta,
			headers: attr.Headers,
		}
	}()

	webhookQueueCounter.Inc()
	return nil
}

func (s *Notifier) Worker(ctx context.Context) {
	defer s.wg.Done()
	for {
		select {
		case event := <-s.eventChan:
			client := &http.Client{}
			payload := webhookPayload{
				JobName:     event.meta.JobName.String(),
				Project:     event.meta.Tenant.ProjectName().String(),
				Namespace:   event.meta.Tenant.NamespaceName().String(),
				Destination: event.jobMeta.DestinationURN,
				ScheduledAt: event.meta.JobScheduledAt.String(),
				Status:      event.meta.Status.String(),
				JobLabel:    event.jobMeta.Labels,
			}
			payloadJSON, err := json.Marshal(payload)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w", err)
				continue
			}

			req, err := http.NewRequestWithContext(ctx, http.MethodPost, event.url, bytes.NewBuffer(payloadJSON))
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w", err)
				continue
			}
			req.Header.Add("Content-Type", "application/json")
			for name, value := range event.headers {
				req.Header.Add(name, value)
			}

			res, err := client.Do(req)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w", err)
				continue
			}
			webhookWorkerSendCounter.Inc()

			err = res.Body.Close()
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w", err)
				continue
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
