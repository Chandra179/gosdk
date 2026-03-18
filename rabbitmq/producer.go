package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Producer handles message publishing with confirms and OTel tracing
type Producer struct {
	client *Client
	tracer trace.Tracer
}

// NewProducer creates a new Producer instance
func NewProducer(client *Client) *Producer {
	return &Producer{
		client: client,
		tracer: otel.Tracer("rabbitmq-producer"),
	}
}

// PublishOptions contains options for publishing a message
type PublishOptions struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Headers    map[string]interface{}
}

// PublishMessage publishes a message to the specified exchange/routing key
func (p *Producer) PublishMessage(ctx context.Context, opts PublishOptions, payload interface{}) error {
	// Start a span for the publish operation
	ctx, span := p.tracer.Start(ctx, "rabbitmq.publish",
		trace.WithAttributes(
		// Add standard messaging attributes
		),
	)
	defer span.End()

	// Serialize payload
	body, err := json.Marshal(payload)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Get publisher confirms channel
	ch, err := p.client.GetPublisherChannel()
	if err != nil {
		span.RecordError(err)
		// Check if channel is closed, trigger reconnection and retry once
		if strings.Contains(err.Error(), "channel is closed") {
			p.client.triggerReconnect()
			// Wait for reconnection with timeout (up to 10 seconds)
			for i := 0; i < 20; i++ {
				time.Sleep(500 * time.Millisecond)
				if p.client.IsHealthy() {
					break
				}
			}
			ch, err = p.client.GetPublisherChannel()
			if err != nil {
				span.RecordError(err)
				return fmt.Errorf("failed to get publisher channel after retry: %w", err)
			}
		} else {
			return fmt.Errorf("failed to get publisher channel: %w", err)
		}
	}
	defer p.client.ReturnPublisherChannel(ch)

	// Prepare headers with OTel trace context
	headers := amqp.Table{}
	if opts.Headers != nil {
		for k, v := range opts.Headers {
			headers[k] = v
		}
	}

	// Inject trace context into headers
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	for k, v := range carrier {
		headers[k] = v
	}

	// Determine delivery mode
	deliveryMode := amqp.Transient
	if p.client.config.PersistentDelivery {
		deliveryMode = amqp.Persistent
	}

	// Set mandatory flag
	mandatory := p.client.config.Mandatory
	if opts.Mandatory {
		mandatory = true
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: deliveryMode,
		Headers:      headers,
	}

	// If publisher confirms are enabled, wait for confirmation
	if p.client.config.PublisherConfirms {
		confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

		err = ch.PublishWithContext(
			ctx,
			opts.Exchange,
			opts.RoutingKey,
			mandatory,
			opts.Immediate,
			publishing,
		)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to publish message: %w", err)
		}

		// Wait for confirmation
		select {
		case confirm := <-confirms:
			if !confirm.Ack {
				err := fmt.Errorf("message was nacked by broker")
				span.RecordError(err)
				return err
			}
		case <-ctx.Done():
			span.RecordError(ctx.Err())
			return ctx.Err()
		}
	} else {
		err = ch.PublishWithContext(
			ctx,
			opts.Exchange,
			opts.RoutingKey,
			mandatory,
			opts.Immediate,
			publishing,
		)
		if err != nil {
			span.RecordError(err)
			return fmt.Errorf("failed to publish message: %w", err)
		}
	}

	p.client.logger.Debug("message published successfully",
		"exchange", opts.Exchange,
		"routing_key", opts.RoutingKey,
	)

	return nil
}

// SendMessage is a convenience method for sending to a direct queue
func (p *Producer) SendMessage(ctx context.Context, queueName string, payload interface{}) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   "", // Default exchange
		RoutingKey: queueName,
	}, payload)
}

// SendMessageWithHeaders publishes a message with custom headers
func (p *Producer) SendMessageWithHeaders(ctx context.Context, queueName string, payload interface{}, headers map[string]interface{}) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   "",
		RoutingKey: queueName,
		Headers:    headers,
	}, payload)
}

// SendToTopic publishes a message to a topic exchange
func (p *Producer) SendToTopic(ctx context.Context, exchange, routingKey string, payload interface{}) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
	}, payload)
}

// SendToTopicWithHeaders publishes a message to a topic exchange with custom headers
func (p *Producer) SendToTopicWithHeaders(ctx context.Context, exchange, routingKey string, payload interface{}, headers map[string]interface{}) error {
	return p.PublishMessage(ctx, PublishOptions{
		Exchange:   exchange,
		RoutingKey: routingKey,
		Headers:    headers,
	}, payload)
}

// PublishBatch publishes multiple messages in a batch
func (p *Producer) PublishBatch(ctx context.Context, opts PublishOptions, payloads []interface{}) error {
	for i, payload := range payloads {
		if err := p.PublishMessage(ctx, opts, payload); err != nil {
			return fmt.Errorf("failed to publish message %d: %w", i, err)
		}
	}
	return nil
}
