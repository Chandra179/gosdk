package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrNotConnected is returned when an operation is attempted while the client is not connected.
var ErrNotConnected = errors.New("rabbitmq: client not connected")

// Publisher sends messages to RabbitMQ exchanges or queues.
type Publisher interface {
	PublishMessage(ctx context.Context, opts PublishOptions, payload interface{}) error
	SendMessage(ctx context.Context, queueName string, payload interface{}) error
	SendMessageWithHeaders(ctx context.Context, queueName string, payload interface{}, headers map[string]interface{}) error
	SendToTopic(ctx context.Context, exchange, routingKey string, payload interface{}) error
	SendToTopicWithHeaders(ctx context.Context, exchange, routingKey string, payload interface{}, headers map[string]interface{}) error
	PublishBatch(ctx context.Context, opts PublishOptions, payloads []interface{}) error
}

// Subscriber consumes messages from RabbitMQ queues.
type Subscriber interface {
	StartConsumer(ctx context.Context, opts ConsumeOptions, handler MessageHandler) error
	ConsumeWithDefaults(ctx context.Context, queueName string, handler MessageHandler) error
}

// TopologyDeclarer declares queues, exchanges, and bindings.
type TopologyDeclarer interface {
	DeclareQueue(config QueueConfig) (*amqp.Queue, error)
	DeclareExchange(config ExchangeConfig) error
	BindQueue(config BindingConfig) error
	SetupDeadLetterExchange(mainQueueName, dlxName string, retryTTL int) error
	SetupQueueWithDLX(queueName, dlxName string) (*amqp.Queue, error)
}

// ConnectionState represents the current state of the connection.
type ConnectionState int32

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateClosing
)

// Client manages RabbitMQ connections with auto-reconnection.
type Client struct {
	config *Config
	logger *slog.Logger

	publisherConn *amqp.Connection
	consumerConn  *amqp.Connection

	publisherPool *channelPool

	state     atomic.Int32
	closeChan chan struct{}

	topology *TopologyCache

	mu sync.RWMutex
}

// TopologyCache stores declared queues, exchanges, and bindings for reconnection.
type TopologyCache struct {
	queues    []QueueConfig
	exchanges []ExchangeConfig
	bindings  []BindingConfig
	mu        sync.RWMutex
}

// NewClient creates a new RabbitMQ client with auto-reconnection.
func NewClient(config *Config, logger *slog.Logger) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("rabbitmq: config nil")
	}

	if err := config.ApplyDefaults(); err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}

	client := &Client{
		config:        config,
		logger:        logger,
		publisherPool: newChannelPool(config.ChannelPoolSize),
		closeChan:     make(chan struct{}),
		topology: &TopologyCache{
			queues:    make([]QueueConfig, 0),
			exchanges: make([]ExchangeConfig, 0),
			bindings:  make([]BindingConfig, 0),
		},
	}

	client.state.Store(int32(StateDisconnected))

	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connection failed: %w", err)
	}

	return client, nil
}

// connect establishes publisher and consumer connections and fills the channel pool.
func (c *Client) connect() error {
	c.state.Store(int32(StateConnecting))

	pubConn, err := c.dial("publisher")
	if err != nil {
		return fmt.Errorf("publisher connection failed: %w", err)
	}

	consConn, err := c.dial("consumer")
	if err != nil {
		pubConn.Close()
		return fmt.Errorf("consumer connection failed: %w", err)
	}

	// Hold mutex while assigning connection pointers
	c.mu.Lock()
	c.publisherConn = pubConn
	c.consumerConn = consConn
	c.mu.Unlock()

	// Drain stale channels and fill pool from the new connection
	c.publisherPool.drain()
	if err := c.publisherPool.fill(pubConn, c.config.PublisherConfirms); err != nil {
		pubConn.Close()
		consConn.Close()
		return fmt.Errorf("channel pool initialization failed: %w", err)
	}

	c.state.Store(int32(StateConnected))
	c.logger.Info("rabbitmq connected successfully",
		"connection_name", c.config.ConnectionName,
	)

	c.setupNotifyHandlers()

	return nil
}

// dial creates a new AMQP connection with a named connection type.
func (c *Client) dial(connType string) (*amqp.Connection, error) {
	config := amqp.Config{
		Locale: "en_US",
		Properties: amqp.Table{
			"connection_name": fmt.Sprintf("%s-%s", c.config.ConnectionName, connType),
		},
	}

	conn, err := amqp.DialConfig(c.config.URL, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// setupNotifyHandlers registers NotifyClose handlers for both connections.
func (c *Client) setupNotifyHandlers() {
	go c.handleConnectionClose(c.publisherConn, "publisher")
	go c.handleConnectionClose(c.consumerConn, "consumer")
}

// handleConnectionClose watches for a connection close event and triggers reconnection.
func (c *Client) handleConnectionClose(conn *amqp.Connection, connType string) {
	notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-c.closeChan:
		return
	case err, ok := <-notifyClose:
		if !ok || err != nil {
			c.logger.Warn("rabbitmq connection closed",
				"connection_type", connType,
				"error", err,
			)
			go func() {
				c.publisherPool.drain()
				c.triggerReconnect()
			}()
		}
	}
}

// triggerReconnect initiates reconnection if not already connecting or closing.
func (c *Client) triggerReconnect() {
	for {
		currentState := c.state.Load()
		switch ConnectionState(currentState) {
		case StateConnected:
			if c.state.CompareAndSwap(int32(StateConnected), int32(StateConnecting)) {
				go c.reconnectWithBackoff()
				return
			}
		case StateConnecting, StateClosing:
			return
		case StateDisconnected:
			if c.state.CompareAndSwap(int32(StateDisconnected), int32(StateConnecting)) {
				go c.reconnectWithBackoff()
				return
			}
		}
	}
}

// reconnectWithBackoff handles reconnection with exponential backoff.
func (c *Client) reconnectWithBackoff() {
	backoff := c.config.ReconnectInitialInterval
	maxBackoff := c.config.ReconnectMaxInterval

	for {
		select {
		case <-c.closeChan:
			c.state.Store(int32(StateDisconnected))
			return
		case <-time.After(backoff):
			if c.state.Load() == int32(StateClosing) {
				c.logger.Info("client closing, skipping reconnection")
				c.state.Store(int32(StateDisconnected))
				return
			}

			c.logger.Info("attempting reconnection", "backoff", backoff)

			if err := c.connect(); err != nil {
				c.logger.Error("reconnection attempt failed", "error", err, "next_backoff", backoff*2)

				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			if err := c.restoreTopology(); err != nil {
				c.logger.Error("failed to restore topology", "error", err)
			}

			c.logger.Info("reconnection successful")
			return
		}
	}
}

// GetPublisherChannel returns a channel from the publisher pool.
// The context controls how long the caller waits for a channel to become available.
func (c *Client) GetPublisherChannel(ctx context.Context) (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, ErrNotConnected
	}

	return c.publisherPool.get(ctx)
}

// ReturnPublisherChannel returns a channel to the publisher pool.
func (c *Client) ReturnPublisherChannel(ch *amqp.Channel) {
	c.publisherPool.returnCh(ch)
}

// CreateConsumerChannel creates a new channel on the consumer connection.
func (c *Client) CreateConsumerChannel() (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, ErrNotConnected
	}

	c.mu.RLock()
	conn := c.consumerConn
	c.mu.RUnlock()

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	return ch, nil
}

// Close gracefully shuts down all connections and the channel pool.
func (c *Client) Close() error {
	c.state.Store(int32(StateClosing))
	close(c.closeChan)

	c.publisherPool.close()

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.publisherConn != nil && !c.publisherConn.IsClosed() {
		c.publisherConn.Close()
	}

	if c.consumerConn != nil && !c.consumerConn.IsClosed() {
		c.consumerConn.Close()
	}

	c.logger.Info("rabbitmq client closed")
	return nil
}

// IsHealthy returns true if the client is connected and healthy.
func (c *Client) IsHealthy() bool {
	return c.state.Load() == int32(StateConnected)
}

// GetState returns the current connection state.
func (c *Client) GetState() ConnectionState {
	return ConnectionState(c.state.Load())
}
