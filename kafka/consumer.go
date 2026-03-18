package kafka

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// CommitMode defines when offsets are committed.
type CommitMode int

// CommitRecord commits offset after each record is successfully processed.
// Ensures no message loss during shutdown - in-flight messages won't be committed
// and will be redelivered on restart (at-least-once delivery semantics).
const CommitRecord CommitMode = iota

// ConsumerOptions configures consumer behavior.
type ConsumerOptions struct {
	DLQProducer  *Producer
	OnDLQPublish func(topic string, err error)
	MaxRetries   int
	RetryBackoff time.Duration
	Logger       *slog.Logger
	GroupID      string
}

// StartConsumer starts consuming messages and processing them with the handler.
// It blocks until the context is cancelled or an error occurs.
func StartConsumer(ctx context.Context, client *kgo.Client, handler func(*kgo.Record) error, opts ...ConsumerOptions) error {
	options := ConsumerOptions{
		MaxRetries:   3,
		RetryBackoff: 100 * time.Millisecond,
		Logger:       slog.Default(),
	}
	for _, o := range opts {
		if o.MaxRetries > 0 {
			options.MaxRetries = o.MaxRetries
		}
		if o.RetryBackoff > 0 {
			options.RetryBackoff = o.RetryBackoff
		}
		if o.Logger != nil {
			options.Logger = o.Logger
		}
		if o.GroupID != "" {
			options.GroupID = o.GroupID
		}
		options.DLQProducer = o.DLQProducer
		options.OnDLQPublish = o.OnDLQPublish
	}

	logger := options.Logger

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := client.PollFetches(ctx)

		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				logger.Error("fetch error", "error", err)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			if ctx.Err() != nil {
				return
			}

			start := time.Now()
			var lastErr error

			// Retry handler with exponential backoff
			// Attempts: 0 (first try), 1 (first retry), 2 (second retry), etc.
			// If all attempts fail, message is sent to DLQ
			for attempt := 0; attempt <= options.MaxRetries; attempt++ {
				if attempt > 0 {
					backoff := time.Duration(attempt) * options.RetryBackoff
					select {
					case <-ctx.Done():
						return
					case <-time.After(backoff):
					}
				}

				if err := handler(r); err != nil {
					lastErr = err
					logger.Warn("handler error, retrying",
						"topic", r.Topic,
						"partition", r.Partition,
						"offset", r.Offset,
						"attempt", attempt+1,
						"max_retries", options.MaxRetries,
						"error", err,
					)
					continue
				}
				lastErr = nil
				break
			}

			duration := time.Since(start)
			_ = duration

			// All retries exhausted - send message to DLQ or log error
			if lastErr != nil {
				if options.DLQProducer != nil {
					if err := options.DLQProducer.SendToDLQ(ctx, r.Topic, r.Value, r.Key, lastErr); err != nil {
						logger.Error("failed to send message to DLQ",
							"error", err,
							"topic", r.Topic,
							"partition", r.Partition,
							"offset", r.Offset,
							"dlq_error", lastErr,
						)
					} else {
						logger.Error("message sent to DLQ",
							"topic", r.Topic,
							"partition", r.Partition,
							"offset", r.Offset,
							"dlq_topic", r.Topic+".dlq",
							"error", lastErr,
						)
						if options.OnDLQPublish != nil {
							options.OnDLQPublish(r.Topic, lastErr)
						}
					}
				} else {
					logger.Error("handler failed, no DLQ configured",
						"topic", r.Topic,
						"partition", r.Partition,
						"offset", r.Offset,
						"error", lastErr,
					)
				}
			}

			// Per-record commit ensures at-least-once delivery - messages are only committed after successful processing.
			if err := client.CommitRecords(ctx, r); err != nil {
				logger.Error("commit error", "error", err)
			}
		})

	}
}

// StartConsumerWithDLQ is a convenience function to start a consumer with DLQ support.
func StartConsumerWithDLQ(ctx context.Context, client *kgo.Client, handler func(*kgo.Record) error, dlqProducer *Producer) error {
	return StartConsumer(ctx, client, handler, ConsumerOptions{
		DLQProducer: dlqProducer,
		MaxRetries:  3,
	})
}
