package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Message represents a message to be produced to Kafka.
type Message struct {
	Topic    string
	Payload  any
	Key      string
	Headers  map[string]string
	Callback func(*kgo.Record, error)
}

// Producer wraps a Kafka client for producing messages.
type Producer struct {
	client *kgo.Client
	logger *slog.Logger
}

// ProducerOption configures a Producer.
type ProducerOption func(*Producer)

// WithProducerLogger sets the logger for the producer.
func WithProducerLogger(logger *slog.Logger) ProducerOption {
	return func(p *Producer) {
		p.logger = logger
	}
}

// NewProducer creates a new Producer with the given client and options.
func NewProducer(client *kgo.Client, opts ...ProducerOption) *Producer {
	p := &Producer{
		client: client,
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Produce sends a message asynchronously.
func (p *Producer) Produce(ctx context.Context, msg *Message) error {
	data, err := p.marshalPayload(msg.Payload)
	if err != nil {
		return err
	}

	headers := make([]kgo.RecordHeader, 0, len(msg.Headers))
	for k, v := range msg.Headers {
		headers = append(headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	record := &kgo.Record{
		Topic:   msg.Topic,
		Key:     []byte(msg.Key),
		Value:   data,
		Headers: headers,
	}

	callback := msg.Callback
	if callback == nil {
		callback = func(r *kgo.Record, err error) {
			if err != nil {
				p.logger.Error("produce error",
					"topic", r.Topic,
					"error", err,
				)
			}
		}
	}

	p.client.Produce(ctx, record, callback)

	return nil
}

// SendMessage is a convenience method to send a message with just topic and payload.
func (p *Producer) SendMessage(ctx context.Context, topic string, payload any) error {
	return p.Produce(ctx, &Message{Topic: topic, Payload: payload})
}

// SendToDLQ sends a message to the dead letter queue asynchronously.
func (p *Producer) SendToDLQ(ctx context.Context, originalTopic string, value []byte, key []byte, dlqErr error) error {
	dlqTopic := originalTopic + ".dlq"

	headers := map[string]string{
		"origin_topic":  originalTopic,
		"dlq_timestamp": time.Now().Format(time.RFC3339),
		"dlq_error":     dlqErr.Error(),
	}

	var keyStr string
	if len(key) > 0 {
		keyStr = string(key)
	}

	return p.Produce(ctx, &Message{
		Topic:   dlqTopic,
		Payload: value,
		Key:     keyStr,
		Headers: headers,
	})
}

func (p *Producer) marshalPayload(payload any) ([]byte, error) {
	if b, ok := payload.([]byte); ok {
		return b, nil
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}
	return data, nil
}
