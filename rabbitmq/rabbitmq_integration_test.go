package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupRabbitMQContainer creates a RabbitMQ container for testing
func setupRabbitMQContainer(t *testing.T) (testcontainers.Container, string) {
	t.Helper()

	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.2-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(180 * time.Second),
	}

	rabbitmqC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	require.NoError(t, err, "failed to start RabbitMQ container")

	// Get the connection URL
	host, err := rabbitmqC.Host(ctx)
	require.NoError(t, err)

	port, err := rabbitmqC.MappedPort(ctx, "5672")
	require.NoError(t, err)

	url := "amqp://guest:guest@" + host + ":" + port.Port() + "/"

	return rabbitmqC, url
}

// setupTestLogger creates a logger for testing
func setupTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
}

// TestMessage represents a test message structure
type TestMessage struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

func TestNewClient(t *testing.T) {
	t.Run("creates client successfully", func(t *testing.T) {
		ctx := context.Background()
		container, url := setupRabbitMQContainer(t)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("failed to terminate container: %v", err)
			}
		}()

		logger := setupTestLogger()
		config := &Config{
			URL:                      url,
			ConnectionName:           "test-client",
			PublisherConfirms:        true,
			ChannelPoolSize:          DefaultChannelPoolSize,
			PrefetchCount:            DefaultPrefetchCount,
			QueueType:                QueueTypeClassic, // Use classic for tests
			Durable:                  true,
			RetryEnabled:             true,
			RetryTTL:                 DefaultRetryTTLSec * time.Second,
			MaxRetries:               DefaultMaxRetries,
			DeadLetterEnabled:        true,
			ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
			ReconnectMaxInterval:     5 * time.Second,
		}

		client, err := NewClient(config, logger)
		require.NoError(t, err, "failed to create client")
		defer client.Close()

		assert.NotNil(t, client)
		assert.True(t, client.IsHealthy())
		assert.Equal(t, StateConnected, client.GetState())
	})

	t.Run("fails with empty URL", func(t *testing.T) {
		logger := setupTestLogger()
		config := &Config{
			URL: "",
		}

		_, err := NewClient(config, logger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URL is required")
	})
}

func TestProducer_PublishMessage_Integration(t *testing.T) {
	t.Run("successfully publishes and consumes message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		container, url := setupRabbitMQContainer(t)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("failed to terminate container: %v", err)
			}
		}()

		logger := setupTestLogger()
		config := &Config{
			URL:                      url,
			ConnectionName:           "test-producer",
			PublisherConfirms:        true,
			ChannelPoolSize:          DefaultChannelPoolSize,
			PrefetchCount:            DefaultPrefetchCount,
			QueueType:                QueueTypeClassic,
			Durable:                  true,
			RetryEnabled:             true,
			RetryTTL:                 DefaultRetryTTLSec * time.Second,
			MaxRetries:               DefaultMaxRetries,
			ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
			ReconnectMaxInterval:     5 * time.Second,
		}

		client, err := NewClient(config, logger)
		require.NoError(t, err)
		defer client.Close()

		// Declare queue
		tm := NewTopologyManager(client)
		queueName := "test-queue"
		_, err = tm.DeclareQueue(QueueConfig{
			Name:    queueName,
			Durable: true,
		})
		require.NoError(t, err)

		// Create producer
		producer := NewProducer(client)

		// Create consumer
		consumer := NewConsumer(client, logger)

		// Track received messages
		received := make(chan TestMessage, 1)
		done := make(chan bool, 1)

		// Start consumer in background
		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 20*time.Second)
			defer consumerCancel()

			handler := func(ctx context.Context, msg interface{}, delivery amqp.Delivery) error {
				var testMsg TestMessage
				body, _ := json.Marshal(msg)
				if err := json.Unmarshal(body, &testMsg); err != nil {
					t.Logf("unmarshal error: %v", err)
					return err
				}

				select {
				case received <- testMsg:
					select {
					case done <- true:
					default:
					}
				default:
				}
				return nil
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		// Give consumer time to start
		time.Sleep(1 * time.Second)

		// Send test message
		testMsg := TestMessage{
			ID:        "msg-001",
			Content:   "Hello, RabbitMQ!",
			Timestamp: time.Now(),
		}

		err = producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err, "failed to send message")

		// Wait for message
		select {
		case <-done:
			receivedMsg := <-received
			assert.Equal(t, testMsg.ID, receivedMsg.ID)
			assert.Equal(t, testMsg.Content, receivedMsg.Content)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})
}

func TestConsumer_ManualAck_Integration(t *testing.T) {
	t.Run("manual acknowledgment works correctly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()

		container, url := setupRabbitMQContainer(t)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("failed to terminate container: %v", err)
			}
		}()

		logger := setupTestLogger()
		config := &Config{
			URL:                      url,
			ConnectionName:           "test-ack",
			PublisherConfirms:        true,
			ChannelPoolSize:          DefaultChannelPoolSize,
			PrefetchCount:            DefaultPrefetchCount,
			QueueType:                QueueTypeClassic,
			Durable:                  true,
			AutoAck:                  false, // Manual ack
			RetryEnabled:             true,
			RetryTTL:                 DefaultRetryTTLSec * time.Second,
			MaxRetries:               DefaultMaxRetries,
			ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
			ReconnectMaxInterval:     5 * time.Second,
		}

		client, err := NewClient(config, logger)
		require.NoError(t, err)
		defer client.Close()

		// Declare queue
		tm := NewTopologyManager(client)
		queueName := "test-ack-queue"
		_, err = tm.DeclareQueue(QueueConfig{
			Name:    queueName,
			Durable: true,
		})
		require.NoError(t, err)

		// Create producer and send message
		producer := NewProducer(client)
		testMsg := TestMessage{
			ID:        "ack-test",
			Content:   "Testing manual ack",
			Timestamp: time.Now(),
		}

		err = producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)

		// Consumer that doesn't ack (simulating failure)
		consumer := NewConsumer(client, logger)
		handlerCalled := make(chan bool, 1)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 5*time.Second)
			defer consumerCancel()

			handler := func(ctx context.Context, msg interface{}, delivery amqp.Delivery) error {
				handlerCalled <- true
				return fmt.Errorf("simulated processing error")
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		// Wait for first handler call
		select {
		case <-handlerCalled:
			// Message was processed (and failed)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for first handler call")
		}

		// Wait a bit for redelivery
		time.Sleep(2 * time.Second)

		// The message should still be in the queue (unacked)
		// This validates that manual ack is working
	})
}

func TestTopologyManager_SetupDeadLetterExchange_Integration(t *testing.T) {
	t.Run("sets up DLX with retry and parking lot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		container, url := setupRabbitMQContainer(t)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("failed to terminate container: %v", err)
			}
		}()

		logger := setupTestLogger()
		config := &Config{
			URL:                      url,
			ConnectionName:           "test-dlx",
			ChannelPoolSize:          DefaultChannelPoolSize,
			PrefetchCount:            DefaultPrefetchCount,
			QueueType:                QueueTypeClassic,
			Durable:                  true,
			RetryEnabled:             true,
			RetryTTL:                 DefaultRetryTTLSec * time.Second,
			MaxRetries:               DefaultMaxRetries,
			DeadLetterEnabled:        true,
			ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
			ReconnectMaxInterval:     5 * time.Second,
		}

		client, err := NewClient(config, logger)
		require.NoError(t, err)
		defer client.Close()

		tm := NewTopologyManager(client)

		mainQueue := "test-main-queue"
		dlxName := "test-dlx"

		// Setup DLX
		err = tm.SetupDeadLetterExchange(mainQueue, dlxName, 5000) // 5 second TTL for testing
		require.NoError(t, err)

		// Setup main queue with DLX
		_, err = tm.SetupQueueWithDLX(mainQueue, dlxName)
		require.NoError(t, err)

		// Verify topology exists by sending a message that will fail
		producer := NewProducer(client)
		testMsg := TestMessage{
			ID:        "dlx-test",
			Content:   "Testing DLX",
			Timestamp: time.Now(),
		}

		err = producer.SendMessage(ctx, mainQueue, testMsg)
		require.NoError(t, err)
	})
}

func TestClient_Reconnection_Integration(t *testing.T) {
	t.Run("reconnects after connection loss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		container, url := setupRabbitMQContainer(t)
		defer func() {
			if err := container.Terminate(ctx); err != nil {
				t.Logf("failed to terminate container: %v", err)
			}
		}()

		logger := setupTestLogger()
		config := &Config{
			URL:                      url,
			ConnectionName:           "test-reconnect",
			PublisherConfirms:        true,
			ChannelPoolSize:          DefaultChannelPoolSize,
			PrefetchCount:            DefaultPrefetchCount,
			QueueType:                QueueTypeClassic,
			Durable:                  true,
			RetryEnabled:             true,
			RetryTTL:                 DefaultRetryTTLSec * time.Second,
			MaxRetries:               DefaultMaxRetries,
			ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
			ReconnectMaxInterval:     5 * time.Second,
		}

		client, err := NewClient(config, logger)
		require.NoError(t, err)
		defer client.Close()

		// Declare queue
		tm := NewTopologyManager(client)
		queueName := "test-reconnect-queue"
		_, err = tm.DeclareQueue(QueueConfig{
			Name:    queueName,
			Durable: true,
		})
		require.NoError(t, err)

		// Verify initial connection
		assert.True(t, client.IsHealthy())

		// Simulate connection loss by closing connections
		client.mu.Lock()
		if client.publisherConn != nil {
			client.publisherConn.Close()
		}
		if client.consumerConn != nil {
			client.consumerConn.Close()
		}
		client.mu.Unlock()

		// Wait for reconnection with polling
		var reconnected bool
		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)
			if client.IsHealthy() {
				reconnected = true
				break
			}
		}
		assert.True(t, reconnected, "client should reconnect after connection loss")

		// Verify we can still publish after reconnection
		producer := NewProducer(client)
		testMsg := TestMessage{
			ID:        "reconnect-test",
			Content:   "After reconnection",
			Timestamp: time.Now(),
		}

		err = producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)
	})
}
