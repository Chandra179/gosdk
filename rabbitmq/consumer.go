package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// MessageHandler is called for each consumed message
type MessageHandler func(ctx context.Context, msg interface{}, delivery amqp.Delivery) error

// Consumer handles message consumption with QoS and OTel tracing
type Consumer struct {
	client *Client
	logger *slog.Logger
	tracer trace.Tracer
}

// NewConsumer creates a new Consumer instance
func NewConsumer(client *Client, logger *slog.Logger) *Consumer {
	return &Consumer{
		client: client,
		logger: logger,
		tracer: otel.Tracer("rabbitmq-consumer"),
	}
}

// ConsumeOptions contains options for consuming messages
type ConsumeOptions struct {
	QueueName string
	Consumer  string // Consumer tag
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      map[string]interface{}
}

// StartConsumer starts consuming messages from a queue
func (c *Consumer) StartConsumer(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error {
	ch, err := c.client.CreateConsumerChannel()
	if err != nil {
		return fmt.Errorf("failed to create consumer channel: %w", err)
	}

	// Set QoS (prefetch count) - critical for production
	prefetchCount := c.client.config.PrefetchCount
	if prefetchCount <= 0 {
		return fmt.Errorf("prefetchCount < 0")
	}

	if err := ch.Qos(prefetchCount, 0, false); err != nil {
		ch.Close()
		return fmt.Errorf("failed to set QoS: %w", err)
	}

	// Use manual ack by default (production best practice)
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

	// Process messages
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

// processDelivery processes a single delivery
func (c *Consumer) processDelivery(ctx context.Context, delivery amqp.Delivery, handler MessageHandler, autoAck bool) error {
	// Extract trace context from headers
	carrier := propagation.MapCarrier{}
	for k, v := range delivery.Headers {
		if strVal, ok := v.(string); ok {
			carrier[k] = strVal
		}
	}

	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)

	// Start a span for processing
	ctx, span := c.tracer.Start(ctx, "rabbitmq.consume",
		trace.WithAttributes(
		// Add messaging attributes
		),
	)
	defer span.End()

	// Unmarshal message
	var msg interface{}
	if err := json.Unmarshal(delivery.Body, &msg); err != nil {
		span.RecordError(err)

		// Reject the message (don't requeue - it's a poison pill)
		if !autoAck {
			if rejectErr := delivery.Reject(false); rejectErr != nil {
				c.logger.Error("failed to reject message", "error", rejectErr)
			}
		}
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Call the handler
	if err := handler(ctx, msg, delivery); err != nil {
		span.RecordError(err)

		if !autoAck {
			// Check retry count from x-death header
			retryCount := getRetryCount(delivery)
			maxRetries := c.client.config.MaxRetries

			if retryCount >= maxRetries {
				// Move to parking lot
				if rejectErr := delivery.Reject(false); rejectErr != nil {
					c.logger.Error("failed to reject message to parking lot", "error", rejectErr)
				}
				c.logger.Warn("message moved to parking lot (max retries exceeded)",
					"retry_count", retryCount,
					"max_retries", maxRetries,
				)
			} else {
				// Requeue for retry
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

	// Acknowledge successful processing
	if !autoAck {
		if err := delivery.Ack(false); err != nil {
			c.logger.Error("failed to acknowledge message", "error", err)
			return fmt.Errorf("failed to ack: %w", err)
		}
	}

	return nil
}

// getRetryCount extracts the retry count from x-death header
func getRetryCount(delivery amqp.Delivery) int {
	if xDeath, ok := delivery.Headers["x-death"].([]interface{}); ok {
		return len(xDeath)
	}
	return 0
}

// ConsumeWithDefaults starts consuming with sensible defaults
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

// BatchConsumer handles batch message consumption
type BatchConsumer struct {
	consumer      *Consumer
	batchSize     int
	flushInterval int
}

// NewBatchConsumer creates a batch consumer
func NewBatchConsumer(consumer *Consumer, batchSize, flushInterval int) *BatchConsumer {
	return &BatchConsumer{
		consumer:      consumer,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

// BatchHandler is called with a batch of messages
type BatchHandler func(ctx context.Context, messages []interface{}, deliveries []amqp.Delivery) error

// StartBatchConsumer starts consuming messages in batches
func (bc *BatchConsumer) StartBatchConsumer(ctx context.Context, opts ConsumeOptions, handler BatchHandler) error {
	var batch []interface{}
	var deliveries []amqp.Delivery

	msgHandler := func(ctx context.Context, msg interface{}, delivery amqp.Delivery) error {
		batch = append(batch, msg)
		deliveries = append(deliveries, delivery)

		if len(batch) >= bc.batchSize {
			if err := handler(ctx, batch, deliveries); err != nil {
				return err
			}
			// Acknowledge all messages in batch
			for _, d := range deliveries {
				if err := d.Ack(false); err != nil {
					bc.consumer.logger.Error("failed to ack batch message", "error", err)
				}
			}
			batch = nil
			deliveries = nil
		}
		return nil
	}

	return bc.consumer.StartConsumer(ctx, opts, msgHandler)
}
