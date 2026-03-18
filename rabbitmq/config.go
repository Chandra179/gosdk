package rabbitmq

import (
	"errors"
	"time"
)

// ErrURLRequired is returned when the RabbitMQ URL is not provided
var ErrURLRequired = errors.New("rabbitmq: URL is required")

// QueueType represents the type of RabbitMQ queue
type QueueType string

const (
	// QueueTypeQuorum uses Raft consensus for data safety (recommended for production)
	QueueTypeQuorum QueueType = "quorum"
	// QueueTypeClassic is the traditional queue type (use only for transient data)
	QueueTypeClassic QueueType = "classic"
)

// Default values for RabbitMQ configuration
const (
	DefaultChannelPoolSize     = 10
	DefaultPrefetchCount       = 10
	DefaultReconnectInitialSec = 1
	DefaultReconnectMaxSec     = 60
	DefaultRetryTTLSec         = 30
	DefaultMaxRetries          = 3
)

// Config holds RabbitMQ configuration
type Config struct {
	// Connection settings
	URL            string
	ConnectionName string

	// Producer settings
	PublisherConfirms  bool
	ChannelPoolSize    int
	Mandatory          bool
	PersistentDelivery bool

	// Consumer settings
	PrefetchCount int
	AutoAck       bool // Should always be false in production

	// Queue settings
	QueueType  QueueType
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool

	// Retry/DLX settings
	RetryEnabled      bool
	RetryTTL          time.Duration
	MaxRetries        int
	DeadLetterEnabled bool

	// Reconnection settings
	ReconnectInitialInterval time.Duration
	ReconnectMaxInterval     time.Duration
}

// NewDefaultConfig returns a Config with default values applied
func NewDefaultConfig() *Config {
	return &Config{
		ConnectionName:           "go-service",
		PublisherConfirms:        false,
		ChannelPoolSize:          DefaultChannelPoolSize,
		Mandatory:                false,
		PersistentDelivery:       false,
		PrefetchCount:            DefaultPrefetchCount,
		AutoAck:                  false,
		QueueType:                QueueTypeQuorum,
		Durable:                  true,
		AutoDelete:               false,
		Exclusive:                false,
		NoWait:                   false,
		RetryEnabled:             false,
		RetryTTL:                 DefaultRetryTTLSec * time.Second,
		MaxRetries:               DefaultMaxRetries,
		DeadLetterEnabled:        false,
		ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
		ReconnectMaxInterval:     DefaultReconnectMaxSec * time.Second,
	}
}

// ApplyDefaults validates required config fields.
// Returns an error if any required field is not set.
// Note: All fields are required - values must be loaded from the YAML configuration.
func (c *Config) ApplyDefaults() error {
	if c.URL == "" {
		return errors.New("rabbitmq: URL is required")
	}

	if c.ConnectionName == "" {
		return errors.New("rabbitmq: connection name is required")
	}

	if c.ChannelPoolSize <= 0 {
		return errors.New("rabbitmq: channel pool size is required")
	}

	if c.PrefetchCount <= 0 {
		return errors.New("rabbitmq: prefetch count is required")
	}

	if c.QueueType == "" {
		return errors.New("rabbitmq: queue type is required")
	}

	if c.RetryTTL <= 0 {
		return errors.New("rabbitmq: retry TTL is required")
	}

	if c.MaxRetries <= 0 {
		return errors.New("rabbitmq: max retries is required")
	}

	if c.ReconnectInitialInterval <= 0 {
		return errors.New("rabbitmq: reconnect initial interval is required")
	}

	if c.ReconnectMaxInterval <= 0 {
		return errors.New("rabbitmq: reconnect max interval is required")
	}

	return nil
}

// QueueConfig represents configuration for a specific queue
type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       map[string]interface{}
}

// ExchangeConfig represents configuration for an exchange
type ExchangeConfig struct {
	Name       string
	Kind       string // direct, fanout, topic, headers
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       map[string]interface{}
}

// BindingConfig represents a queue binding
type BindingConfig struct {
	QueueName  string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Args       map[string]interface{}
}
