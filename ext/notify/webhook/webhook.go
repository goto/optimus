package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/goto/optimus/core/scheduler"
)

const (
	httpChannelBuffer = 100
)

var (
	notifierType     = "webhook"
	httpQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationQueue,
		ConstLabels: map[string]string{"type": notifierType},
	})

	httpWorkerSendErrCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name:        scheduler.MetricNotificationWorkerSendErr,
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
	meta    *scheduler.Event
	jobMeta *scheduler.JobRunMeta
}

type webhookPayload struct {
	JobName        string            `json:"job_name"`
	Tenant         string            `json:"tenant"`
	DestinationURN string            `json:"destination_URN"`
	ScheduledAt    string            `json:"scheduled_at"`
	Status         string            `json:"status"`
	JobLabel       map[string]string `json:"job_label"`
}

func (s *Notifier) Notify(_ context.Context, attr scheduler.NotifyAttrs) error { //nolint: gocritic
	go func() {
		s.eventChan <- event{url: attr.Route, meta: attr.JobEvent, jobMeta: attr.Meta}
	}()

	httpQueueCounter.Inc()
	return nil
}

func (s *Notifier) Worker(_ context.Context) {
	defer s.wg.Done()
	for {
		select {
		case event := <-s.eventChan:
			client := &http.Client{}
			payload := webhookPayload{
				JobName:        event.meta.JobName.String(),
				Tenant:         event.meta.Tenant.ProjectName().String() + ":" + event.meta.Tenant.NamespaceName().String(),
				DestinationURN: event.jobMeta.DestinationURN,
				ScheduledAt:    event.meta.JobScheduledAt.String(),
				Status:         event.meta.Status.String(),
				JobLabel:       event.jobMeta.Labels,
			}
			payloadJson, err := json.Marshal(payload)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w\n", err)
				continue
			}

			req, err := http.NewRequest(http.MethodPost, event.url, bytes.NewBuffer(payloadJson))
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w\n", err)
				continue
			}
			req.Header.Add("Content-Type", "application/json")

			res, err := client.Do(req)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w\n", err)
				continue
			}

			_, err = ioutil.ReadAll(res.Body)
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w\n", err)
				continue
			}

			err = res.Body.Close()
			if err != nil {
				s.workerErrChan <- fmt.Errorf("webhook worker: %w\n", err)
				continue
			}
		default:
			// send messages in batches of 5 secs
			time.Sleep(s.eventBatchInterval)
		}
	}
}

func (s *Notifier) Close() error { // nolint: unparam
	close(s.workerErrChan)
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
			httpWorkerSendErrCounter.Inc()
		}
		this.wg.Done()
	}()

	this.wg.Add(1)
	go this.Worker(ctx)
	return this
}
