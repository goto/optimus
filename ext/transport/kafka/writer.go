package kafka

import (
	"context"

	"github.com/goto/salt/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

var (
	kafkaQueueCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kafka_publish_queue",
		Help: "Events published to kafka topic",
	})
)

type Writer struct {
	kWriter *kafka.Writer
}

func NewWriter(kafkaBrokerUrls []string, topic string, logger log.Logger) *Writer {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokerUrls...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		Balancer:               &kafka.LeastBytes{},
		RequiredAcks:           kafka.RequireOne,
		Logger:                 kafka.LoggerFunc(logger.Info),
		ErrorLogger:            kafka.LoggerFunc(logger.Error),
	}

	return &Writer{kWriter: writer}
}

func (w *Writer) Close() error {
	return w.kWriter.Close()
}

func (w *Writer) Write(messages [][]byte) error {
	kMessage := make([]kafka.Message, len(messages))
	for i, m := range messages {
		kMessage[i] = kafka.Message{
			Value: m,
		}
	}

	err := w.kWriter.WriteMessages(context.Background(), kMessage...)
	if err == nil {
		kafkaQueueCounter.Add(float64(len(messages)))
		return nil
	}
	return err
}