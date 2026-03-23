package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// MessageHandler is called for each consumed message.
type MessageHandler func(ctx context.Context, msg interface{}, delivery amqp.Delivery) error

// Consumer handles message consumption with QoS and OTel tracing.
type Consumer struct {
	client *Client
	logger *slog.Logger
	tracer trace.Tracer
}

// NewConsumer creates a new Subscriber backed by client.
func NewConsumer(client *Client, logger *slog.Logger) Subscriber {
	return &Consumer{
		client: client,
		logger: logger,
		tracer: otel.Tracer("rabbitmq-consumer"),
	}
}

// ConsumeOptions contains options for consuming messages.
type ConsumeOptions struct {
	QueueName     string
	Consumer      string // Consumer tag
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Args          map[string]interface{}
	AutoReconnect bool // When true, the consumer restarts after delivery channel closure.
}

// StartConsumer starts consuming messages from a queue.
// If opts.AutoReconnect is true, it will automatically restart on delivery channel closure.
func (c *Consumer) StartConsumer(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	if opts.AutoReconnect {
		return c.startConsumerWithReconnect(ctx, opts, handler)
	}
	return c.startConsumeLoop(ctx, opts, handler)
}

// startConsumeLoop runs the core consume loop on a single channel.
func (c *Consumer) startConsumeLoop(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	ch, err := c.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create consumer channel: %w", err)
	}

	prefetchCount := c.client.config.PrefetchCount
	if prefetchCount <= 0 {
		ch.Close()
		return fmt.Errorf("prefetchCount must be > 0")
	}

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	autoAck := c.client.config.AutoAck
	if opts.AutoAck {
		autoAck = true
	}

	deliveries, err := ch.Consume(
		opts.QueueName,
		opts.Consumer,
		autoAck,
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	c.logger.Info("started consuming",
		"queue", opts.QueueName,
		"consumer", opts.Consumer,
		"auto_ack", autoAck,
		"prefetch", prefetchCount,
	)

	for {
		select {
		case <-ctx.Done():
			ch.Close()
			return ctx.Err()

		case delivery, ok := <-deliveries:
			if !ok {
				c.logger.Warn("delivery channel closed")
				ch.Close()
				return fmt.Errorf("delivery channel closed")
			}

			if err := c.processDelivery(ctx, delivery, handler, autoAck); err != nil {
				c.logger.Error("failed to process delivery",
					"error", err,
					"delivery_tag", delivery.DeliveryTag,
				)
			}
		}
	}
}

// startConsumerWithReconnect wraps startConsumeLoop and restarts it after
// delivery channel closures, waiting for the client to reconnect.
func (c *Consumer) startConsumerWithReconnect(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	// Use a copy without AutoReconnect to avoid recursion
	innerOpts := opts
	innerOpts.AutoReconnect = false

	for {
		err := c.startConsumeLoop(ctx, innerOpts, handler)
		if err == nil || ctx.Err() != nil {
			return err
		}

		c.logger.Warn("consumer disconnected, waiting for reconnection",
			"queue", opts.QueueName,
			"error", err,
		)

		if !c.waitForReconnect(ctx) {
			return ctx.Err()
		}

		c.logger.Info("client reconnected, restarting consumer", "queue", opts.QueueName)
	}
}

// waitForReconnect polls until the client is healthy or the context is cancelled.
func (c *Consumer) waitForReconnect(ctx context.Context) bool {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			if c.client.IsHealthy() {
				return true
			}
		}
	}
}

// processDelivery processes a single delivery with tracing and ack management.
func (c *Consumer) processDelivery(ctx context.Context, delivery amqp.Delivery, handler MessageHandler, autoAck bool) error {
	// Extract trace context from headers
	carrier := propagation.MapCarrier{}
	for k, v := range delivery.Headers {
		if strVal, ok := v.(string); ok {
			carrier[k] = strVal
		}
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	ctx, span := c.tracer.Start(ctx, "rabbitmq.consume",
		trace.WithAttributes(),
	)
	defer span.End()

	var msg interface{}
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		span.RecordError(err)

		if !autoAck {
			if rejectErr := delivery.Reject(false); rejectErr != nil {
				c.logger.Error("failed to reject message", "error", rejectErr)
			}
		}
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := handler(ctx, msg, delivery); err != nil {
		span.RecordError(err)

		if !autoAck {
			retryCount := getRetryCount(delivery)
			maxRetries := c.client.config.MaxRetries

			if retryCount >= maxRetries {
				if rejectErr := delivery.Reject(false); rejectErr != nil {
					c.logger.Error("failed to reject message to parking lot", "error", rejectErr)
				}
				c.logger.Warn("message moved to parking lot (max retries exceeded)",
					"retry_count", retryCount,
					"max_retries", maxRetries,
				)
			} else {
				if nackErr := delivery.Nack(false, true); nackErr != nil {
					c.logger.Error("failed to nack message for retry", "error", nackErr)
				}
				c.logger.Debug("message requeued for retry",
					"retry_count", retryCount,
				)
			}
		}
		return fmt.Errorf("handler error: %w", err)
	}

	if !autoAck {
		if err := delivery.Ack(false); err != nil {
			c.logger.Error("failed to acknowledge message", "error", err)
			return fmt.Errorf("failed to ack: %w", err)
		}
	}

	return nil
}

// getRetryCount extracts the retry count from x-death header.
func getRetryCount(delivery amqp.Delivery) int {
	if xDeath, ok := delivery.Headers["x-death"].([]interface{}); ok {
		return len(xDeath)
	}
	return 0
}

// ConsumeWithDefaults starts consuming with sensible defaults.
func (c *Consumer) ConsumeWithDefaults(ctx context.Context, queueName string, handler MessageHandler) error {
	return c.StartConsumer(ctx, ConsumeOptions{
		QueueName: queueName,
		Consumer:  "",
		AutoAck:   false,
		Exclusive: false,
		NoLocal:   false,
		NoWait:    false,
		Args:      nil,
	}, handler)
}

// BatchHandler is called with a batch of messages.
type BatchHandler func(ctx context.Context, messages []interface{}, deliveries []amqp.Delivery) error

// BatchConsumer handles batch message consumption.
// It manages its own AMQP channel and ack lifecycle, avoiding double-ack issues.
type BatchConsumer struct {
	client       *Client
	logger       *slog.Logger
	tracer       trace.Tracer
	batchSize    int
	flushTimeout time.Duration
}

// NewBatchConsumer creates a batch consumer.
func NewBatchConsumer(client *Client, logger *slog.Logger, batchSize int, flushTimeout time.Duration) *BatchConsumer {
	return &BatchConsumer{
		client:       client,
		logger:       logger,
		tracer:       otel.Tracer("rabbitmq-batch-consumer"),
		batchSize:    batchSize,
		flushTimeout: flushTimeout,
	}
}

// StartBatchConsumer starts consuming messages in batches.
// It creates its own channel and handles acking per-batch to avoid double-ack.
func (bc *BatchConsumer) StartBatchConsumer(ctx context.Context, opts ConsumeOptions, handler BatchHandler) error {
	ch, err := bc.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create consumer channel: %w", err)
	}

	prefetchCount := bc.client.config.PrefetchCount
	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	deliveries, err := ch.Consume(
		opts.QueueName,
		opts.Consumer,
		false, // never auto-ack for batch consumer
		opts.Exclusive,
		opts.NoLocal,
		opts.NoWait,
		opts.Args,
	)
	if err != nil {
		ch.Close()
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	bc.logger.Info("batch consumer started",
		"queue", opts.QueueName,
		"batch_size", bc.batchSize,
		"flush_timeout", bc.flushTimeout,
	)

	var batch []interface{}
	var batchDeliveries []amqp.Delivery
	flushTimer := time.NewTimer(bc.flushTimeout)
	defer flushTimer.Stop()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		if err := handler(ctx, batch, batchDeliveries); err != nil {
			// Nack all messages in the failed batch for redelivery
			for _, d := range batchDeliveries {
				if nackErr := d.Nack(false, true); nackErr != nil {
					bc.logger.Error("failed to nack batch message", "error", nackErr)
				}
			}
			batch = nil
			batchDeliveries = nil
			return fmt.Errorf("batch handler error: %w", err)
		}

		// Ack all messages in the successful batch
		for _, d := range batchDeliveries {
			if ackErr := d.Ack(false); ackErr != nil {
				bc.logger.Error("failed to ack batch message", "error", ackErr)
			}
		}

		batch = nil
		batchDeliveries = nil
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			// Flush remaining batch before exiting
			if flushErr := flush(); flushErr != nil {
				bc.logger.Error("failed to flush batch on shutdown", "error", flushErr)
			}
			ch.Close()
			return ctx.Err()

		case <-flushTimer.C:
			if err := flush(); err != nil {
				bc.logger.Error("failed to flush batch on timeout", "error", err)
			}
			flushTimer.Reset(bc.flushTimeout)

		case delivery, ok := <-deliveries:
			if !ok {
				if flushErr := flush(); flushErr != nil {
					bc.logger.Error("failed to flush batch on channel close", "error", flushErr)
				}
				ch.Close()
				return fmt.Errorf("delivery channel closed")
			}

			// Extract trace context
			carrier := propagation.MapCarrier{}
			for k, v := range delivery.Headers {
				if strVal, ok := v.(string); ok {
					carrier[k] = strVal
				}
			}
			msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)
			_, span := bc.tracer.Start(msgCtx, "rabbitmq.batch-consume")

			var msg interface{}
			if err := json.Unmarshal(delivery.Body, &msg); err != nil {
				span.RecordError(err)
				span.End()
				// Reject poison pill without requeue
				if rejectErr := delivery.Reject(false); rejectErr != nil {
					bc.logger.Error("failed to reject poison message", "error", rejectErr)
				}
				continue
			}

			span.End()
			batch = append(batch, msg)
			batchDeliveries = append(batchDeliveries, delivery)

			if len(batch) >= bc.batchSize {
				if err := flush(); err != nil {
					bc.logger.Error("failed to flush full batch", "error", err)
				}
				flushTimer.Reset(bc.flushTimeout)
			}
		}
	}
}
