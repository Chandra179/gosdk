//go:build integration

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

var (
	testContainer testcontainers.Container
	testURL       string
	testLogger    *slog.Logger
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	testLogger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:4.2-management-alpine",
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		WaitingFor:   wait.ForLog("Server startup complete").WithStartupTimeout(180 * time.Second),
	}

	var err error
	testContainer, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		testLogger.Error("failed to start RabbitMQ container", "error", err)
		os.Exit(1)
	}

	host, err := testContainer.Host(ctx)
	if err != nil {
		testLogger.Error("failed to get container host", "error", err)
		os.Exit(1)
	}

	port, err := testContainer.MappedPort(ctx, "5672")
	if err != nil {
		testLogger.Error("failed to get container port", "error", err)
		os.Exit(1)
	}

	testURL = "amqp://guest:guest@" + host + ":" + port.Port() + "/"

	code := m.Run()

	if err := testContainer.Terminate(ctx); err != nil {
		testLogger.Error("failed to terminate container", "error", err)
	}

	os.Exit(code)
}

// TestMessage represents a test message structure.
type TestMessage struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

// newTestClient creates a Client with sensible test defaults and registers t.Cleanup.
func newTestClient(t *testing.T, overrides ...func(*Config)) *Client {
	t.Helper()

	config := &Config{
		URL:                      testURL,
		ConnectionName:           t.Name(),
		PublisherConfirms:        true,
		ChannelPoolSize:          DefaultChannelPoolSize,
		PrefetchCount:            DefaultPrefetchCount,
		QueueType:                QueueTypeClassic, // classic for tests (no quorum needed)
		Durable:                  true,
		AutoAck:                  false,
		RetryEnabled:             true,
		RetryTTL:                 DefaultRetryTTLSec * time.Second,
		MaxRetries:               DefaultMaxRetries,
		DeadLetterEnabled:        true,
		ReconnectInitialInterval: DefaultReconnectInitialSec * time.Second,
		ReconnectMaxInterval:     5 * time.Second,
	}

	for _, fn := range overrides {
		fn(config)
	}

	client, err := NewClient(config, testLogger)
	require.NoError(t, err, "failed to create client")

	t.Cleanup(func() { client.Close() })
	return client
}

// newTestQueue declares an isolated queue and returns its name.
func newTestQueue(t *testing.T, client *Client, name string) string {
	t.Helper()
	tm := NewTopologyManager(client)
	_, err := tm.DeclareQueue(QueueConfig{
		Name:    name,
		Durable: true,
	})
	require.NoError(t, err, "failed to declare queue %s", name)
	return name
}

func TestNewClient(t *testing.T) {
	t.Run("creates client successfully", func(t *testing.T) {
		client := newTestClient(t)

		assert.NotNil(t, client)
		assert.True(t, client.IsHealthy())
		assert.Equal(t, StateConnected, client.GetState())
	})

	t.Run("fails with empty URL", func(t *testing.T) {
		config := &Config{URL: ""}
		_, err := NewClient(config, testLogger)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URL is required")
	})
}

func TestProducer_PublishMessage(t *testing.T) {
	t.Run("publishes and consumes message", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-publish-consume")

		producer := NewProducer(client)
		consumer := NewConsumer(client, testLogger)

		received := make(chan TestMessage, 1)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 20*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, msg interface{}, _ amqp.Delivery) error {
				body, _ := json.Marshal(msg)
				var testMsg TestMessage
				if err := json.Unmarshal(body, &testMsg); err != nil {
					return err
				}
				select {
				case received <- testMsg:
				default:
				}
				return nil
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		time.Sleep(1 * time.Second) // let consumer attach

		testMsg := TestMessage{
			ID:        "msg-001",
			Content:   "Hello, RabbitMQ!",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err, "failed to send message")

		select {
		case got := <-received:
			assert.Equal(t, testMsg.ID, got.ID)
			assert.Equal(t, testMsg.Content, got.Content)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message")
		}
	})
}

func TestConsumer_ManualAck(t *testing.T) {
	t.Run("manual acknowledgment works correctly", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-manual-ack")

		producer := NewProducer(client)
		testMsg := TestMessage{
			ID:        "ack-test",
			Content:   "Testing manual ack",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)

		consumer := NewConsumer(client, testLogger)
		handlerCalled := make(chan bool, 1)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 5*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, _ interface{}, _ amqp.Delivery) error {
				select {
				case handlerCalled <- true:
				default:
				}
				return fmt.Errorf("simulated processing error")
			}

			if err := consumer.ConsumeWithDefaults(consumerCtx, queueName, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		select {
		case <-handlerCalled:
			// Handler was called and returned an error — message should be requeued
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for handler call")
		}
	})
}

func TestTopologyManager_SetupDeadLetterExchange(t *testing.T) {
	t.Run("sets up DLX with retry and parking lot", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		tm := NewTopologyManager(client)

		mainQueue := "test-dlx-main"
		dlxName := "test-dlx"

		err := tm.SetupDeadLetterExchange(mainQueue, dlxName, 5000)
		require.NoError(t, err)

		_, err = tm.SetupQueueWithDLX(mainQueue, dlxName)
		require.NoError(t, err)

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

func TestClient_Reconnection(t *testing.T) {
	t.Run("reconnects after connection loss", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-reconnect")

		assert.True(t, client.IsHealthy())

		// Simulate connection loss by closing connections directly
		client.mu.Lock()
		if client.publisherConn != nil {
			client.publisherConn.Close()
		}
		if client.consumerConn != nil {
			client.consumerConn.Close()
		}
		client.mu.Unlock()

		// Wait for reconnection
		var reconnected bool
		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)
			if client.IsHealthy() {
				reconnected = true
				break
			}
		}
		assert.True(t, reconnected, "client should reconnect after connection loss")

		// Verify publish works after reconnection
		producer := NewProducer(client)
		testMsg := TestMessage{
			ID:        "reconnect-test",
			Content:   "After reconnection",
			Timestamp: time.Now(),
		}

		err := producer.SendMessage(ctx, queueName, testMsg)
		require.NoError(t, err)
	})
}

func TestBatchConsumer(t *testing.T) {
	t.Run("processes messages in batches", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-batch")

		producer := NewProducer(client)

		// Send 5 messages
		for i := 0; i < 5; i++ {
			err := producer.SendMessage(ctx, queueName, TestMessage{
				ID:        fmt.Sprintf("batch-%d", i),
				Content:   "Batch test",
				Timestamp: time.Now(),
			})
			require.NoError(t, err)
		}

		batchReceived := make(chan int, 1)
		bc := NewBatchConsumer(client, testLogger, 5, 10*time.Second)

		go func() {
			consumerCtx, consumerCancel := context.WithTimeout(ctx, 15*time.Second)
			defer consumerCancel()

			handler := func(_ context.Context, messages []interface{}, _ []amqp.Delivery) error {
				select {
				case batchReceived <- len(messages):
				default:
				}
				return nil
			}

			if err := bc.StartBatchConsumer(consumerCtx, ConsumeOptions{QueueName: queueName}, handler); err != nil && err != context.Canceled {
				t.Logf("batch consumer error: %v", err)
			}
		}()

		select {
		case count := <-batchReceived:
			assert.Equal(t, 5, count)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for batch")
		}
	})
}

func TestConsumer_AutoReconnect(t *testing.T) {
	t.Run("consumer restarts after delivery channel close", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		client := newTestClient(t)
		queueName := newTestQueue(t, client, "test-auto-reconnect")

		consumer := NewConsumer(client, testLogger)
		received := make(chan string, 10)

		go func() {
			handler := func(_ context.Context, msg interface{}, _ amqp.Delivery) error {
				body, _ := json.Marshal(msg)
				var testMsg TestMessage
				if err := json.Unmarshal(body, &testMsg); err != nil {
					return err
				}
				received <- testMsg.ID
				return nil
			}

			if err := consumer.StartConsumer(ctx, ConsumeOptions{
				QueueName:     queueName,
				AutoReconnect: true,
			}, handler); err != nil && err != context.Canceled {
				t.Logf("consumer error: %v", err)
			}
		}()

		time.Sleep(1 * time.Second)

		// Send first message
		producer := NewProducer(client)
		err := producer.SendMessage(ctx, queueName, TestMessage{ID: "before-drop", Content: "Pre-disconnect"})
		require.NoError(t, err)

		select {
		case id := <-received:
			assert.Equal(t, "before-drop", id)
		case <-time.After(10 * time.Second):
			t.Fatal("timeout waiting for first message")
		}

		// Simulate connection loss
		client.mu.Lock()
		if client.publisherConn != nil {
			client.publisherConn.Close()
		}
		if client.consumerConn != nil {
			client.consumerConn.Close()
		}
		client.mu.Unlock()

		// Wait for reconnection
		for i := 0; i < 30; i++ {
			time.Sleep(500 * time.Millisecond)
			if client.IsHealthy() {
				break
			}
		}
		require.True(t, client.IsHealthy(), "client should reconnect")

		// Re-declare queue after reconnect (topology restore handles this if declared via TopologyManager)
		newTestQueue(t, client, queueName)

		time.Sleep(1 * time.Second) // let consumer re-attach

		// Send second message
		err = producer.SendMessage(ctx, queueName, TestMessage{ID: "after-reconnect", Content: "Post-disconnect"})
		require.NoError(t, err)

		select {
		case id := <-received:
			assert.Equal(t, "after-reconnect", id)
		case <-time.After(15 * time.Second):
			t.Fatal("timeout waiting for message after reconnection")
		}
	})
}
