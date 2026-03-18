package rabbitmq

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ConnectionState represents the current state of the connection
type ConnectionState int32

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateClosing
)

// Client manages RabbitMQ connections with auto-reconnection
type Client struct {
	config *Config
	logger *slog.Logger

	// Connections
	publisherConn *amqp.Connection
	consumerConn  *amqp.Connection

	// Channel pools
	publisherChannels chan *amqp.Channel

	// State management
	state     atomic.Int32
	closeChan chan struct{}

	// Topology cache for reconnection
	topology *TopologyCache

	mu sync.RWMutex
}

// TopologyCache stores declared queues, exchanges, and bindings for reconnection
type TopologyCache struct {
	queues    []QueueConfig
	exchanges []ExchangeConfig
	bindings  []BindingConfig
	mu        sync.RWMutex
}

// NewClient creates a new RabbitMQ client with auto-reconnection
func NewClient(config *Config, logger *slog.Logger) (*Client, error) {
	if config == nil {
		return nil, fmt.Errorf("rabbitmq: config nil")
	}

	// Apply defaults for any unset values, returns error if required fields are missing
	if err := config.ApplyDefaults(); err != nil {
		return nil, fmt.Errorf("rabbitmq: %w", err)
	}

	client := &Client{
		config:            config,
		logger:            logger,
		publisherChannels: make(chan *amqp.Channel, config.ChannelPoolSize),
		closeChan:         make(chan struct{}),
		topology: &TopologyCache{
			queues:    make([]QueueConfig, 0),
			exchanges: make([]ExchangeConfig, 0),
			bindings:  make([]BindingConfig, 0),
		},
	}

	client.state.Store(int32(StateDisconnected))

	// Initial connection
	if err := client.connect(); err != nil {
		return nil, fmt.Errorf("rabbitmq: initial connection failed: %w", err)
	}

	return client, nil
}

// connect establishes initial connections
func (c *Client) connect() error {
	c.state.Store(int32(StateConnecting))

	pubConn, err := c.dial("publisher")
	if err != nil {
		return fmt.Errorf("publisher connection failed: %w", err)
	}
	c.publisherConn = pubConn

	consConn, err := c.dial("consumer")
	if err != nil {
		c.publisherConn.Close()
		return fmt.Errorf("consumer connection failed: %w", err)
	}
	c.consumerConn = consConn

	// Clear any old channels from pools before reinitializing
	c.clearChannelPools()

	// Initialize channel pools
	if err := c.initializeChannelPools(); err != nil {
		c.publisherConn.Close()
		c.consumerConn.Close()
		return fmt.Errorf("channel pool initialization failed: %w", err)
	}

	c.state.Store(int32(StateConnected))
	c.logger.Info("rabbitmq connected successfully",
		"connection_name", c.config.ConnectionName,
	)

	c.setupNotifyHandlers()

	return nil
}

// dial creates a new connection with the given name
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

// setupNotifyHandlers sets up NotifyClose handlers for both connections
func (c *Client) setupNotifyHandlers() {
	go c.handleConnectionClose(c.publisherConn, "publisher")
	go c.handleConnectionClose(c.consumerConn, "consumer")
}

// handleConnectionClose handles individual connection close events
func (c *Client) handleConnectionClose(conn *amqp.Connection, connType string) {
	notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case <-c.closeChan:
		return
	case err, ok := <-notifyClose:
		// Trigger reconnection when channel closes or error occurs
		if !ok || err != nil {
			c.logger.Warn("rabbitmq connection closed",
				"connection_type", connType,
				"error", err,
			)
			// Clear channel pools in a separate goroutine to avoid blocking
			go func() {
				c.clearChannelPools()
				c.triggerReconnect()
			}()
		}
	}
}

// triggerReconnect initiates reconnection if not already connecting/closing
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

// initializeChannelPools creates initial channel pools
func (c *Client) initializeChannelPools() error {
	// Initialize publisher channels with confirms
	for i := 0; i < c.config.ChannelPoolSize; i++ {
		ch, err := c.publisherConn.Channel()
		if err != nil {
			return fmt.Errorf("failed to create publisher channel: %w", err)
		}

		if c.config.PublisherConfirms {
			if err := ch.Confirm(false); err != nil {
				return fmt.Errorf("failed to enable publisher confirms: %w", err)
			}
		}

		c.publisherChannels <- ch
	}

	return nil
}

// reconnectWithBackoff handles reconnection with exponential backoff
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

			// Clear old channels before reconnecting
			c.clearChannelPools()

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
			c.state.Store(int32(StateConnected))
			return
		}
	}
}

// GetPublisherChannel returns a channel from the publisher pool
func (c *Client) GetPublisherChannel() (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, fmt.Errorf("client not connected")
	}

	select {
	case ch := <-c.publisherChannels:
		if ch != nil && !ch.IsClosed() {
			return ch, nil
		}
		return nil, fmt.Errorf("channel is closed, retry connection")
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("timeout waiting for publisher channel")
	}
}

// ReturnPublisherChannel returns a channel to the publisher pool
func (c *Client) ReturnPublisherChannel(ch *amqp.Channel) {
	if ch == nil || ch.IsClosed() {
		return
	}

	select {
	case c.publisherChannels <- ch:
	default:
		// Pool is full, close the channel
		ch.Close()
	}
}

// CreateConsumerChannel creates a new consumer channel for the given connection
func (c *Client) CreateConsumerChannel() (*amqp.Channel, error) {
	if c.state.Load() != int32(StateConnected) {
		return nil, fmt.Errorf("client not connected")
	}

	ch, err := c.consumerConn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer channel: %w", err)
	}

	return ch, nil
}

// clearChannelPools drains and closes all channels in the publisher pool
func (c *Client) clearChannelPools() {
	// Drain publisher channels
	drainChannelPool(c.publisherChannels)
}

// drainChannelPool drains and closes all channels in a pool
func drainChannelPool(pool chan *amqp.Channel) {
	for {
		select {
		case ch := <-pool:
			if ch != nil && !ch.IsClosed() {
				ch.Close()
			}
		default:
			// Pool is empty
			return
		}
	}
}

// Close gracefully closes all connections
func (c *Client) Close() error {
	c.state.Store(int32(StateClosing))
	close(c.closeChan)

	close(c.publisherChannels)

	for ch := range c.publisherChannels {
		ch.Close()
	}

	// Close connections
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

// IsHealthy returns true if the client is connected and healthy
func (c *Client) IsHealthy() bool {
	return c.state.Load() == int32(StateConnected)
}

// GetState returns the current connection state
func (c *Client) GetState() ConnectionState {
	return ConnectionState(c.state.Load())
}
