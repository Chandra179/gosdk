package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMessage is a sample message type for testing.
type TestMessage struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Created time.Time `json:"created"`
}

// setupKafkaContainer creates a Kafka testcontainer.
func setupKafkaContainer(t *testing.T, ctx context.Context) (*kafka.KafkaContainer, []string) {
	t.Helper()

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithEnv(map[string]string{
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true",
		}),
	)
	require.NoError(t, err, "failed to start Kafka container")

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "failed to get brokers")

	return kafkaContainer, brokers
}

// ensureTopicExists creates a Kafka topic if it doesn't exist.
func ensureTopicExists(ctx context.Context, brokers []string, topic string) error {
	kgoClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer kgoClient.Close()

	adminClient := kadm.NewClient(kgoClient)
	defer adminClient.Close()

	_, err = adminClient.CreateTopics(ctx, 1, 1, nil, topic)
	return err
}

// TestProducerBasic tests basic message production.
func TestProducerBasic(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate container: %v", err)
		}
	}()

	cfg := DefaultConfig(brokers, "test-group-producer")
	client, err := NewKafkaClientFromConfig(ctx, cfg)
	require.NoError(t, err, "failed to create Kafka client")
	defer client.Close()

	producer := NewProducer(client)

	topic := "test-topic-producer"

	err = ensureTopicExists(ctx, brokers, topic)
	require.NoError(t, err, "failed to create topic")

	testMsg := TestMessage{
		ID:      "msg-1",
		Content: "Hello, Kafka!",
		Created: time.Now(),
	}

	err = producer.SendMessage(ctx, topic, testMsg)
	assert.NoError(t, err, "failed to produce message")

	msg := &Message{
		Topic:   topic,
		Payload: testMsg,
		Key:     "test-key-1",
		Headers: map[string]string{
			"source":      "integration-test",
			"version":     "1.0",
			"environment": "test",
		},
	}

	err = producer.Produce(ctx, msg)
	assert.NoError(t, err, "failed to produce message with headers")
}

// TestProducerAsync tests asynchronous message production.
func TestProducerAsync(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	cfg := DefaultConfig(brokers, "test-group-async")
	client, err := NewKafkaClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer client.Close()

	producer := NewProducer(client)
	topic := "test-topic-async"

	err = ensureTopicExists(ctx, brokers, topic)
	require.NoError(t, err, "failed to create topic")

	var wg sync.WaitGroup
	var callbackErr error
	var mu sync.Mutex

	wg.Add(1)
	msg := &Message{
		Topic:   topic,
		Payload: TestMessage{ID: "async-1", Content: "Async message"},
		Callback: func(r *kgo.Record, err error) {
			mu.Lock()
			defer mu.Unlock()
			callbackErr = err
			wg.Done()
		},
	}

	err = producer.Produce(ctx, msg)
	assert.NoError(t, err)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		mu.Lock()
		assert.NoError(t, callbackErr, "callback received error")
		mu.Unlock()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for callback")
	}
}

// TestConsumerBasic tests basic message consumption.
func TestConsumerBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	topic := "test-topic-consumer"

	producerCfg := DefaultConfig(brokers, "")
	producerClient, err := NewKafkaClientFromConfig(ctx, producerCfg)
	require.NoError(t, err)
	defer producerClient.Close()

	producer := NewProducer(producerClient)

	err = ensureTopicExists(ctx, brokers, topic)
	require.NoError(t, err, "failed to create topic")

	consumerCfg := DefaultConfig(brokers, "test-consumer-group").WithTopics(topic)
	consumerClient, err := NewKafkaClientFromConfig(ctx, consumerCfg)
	require.NoError(t, err)
	defer consumerClient.Close()

	testMessages := []TestMessage{
		{ID: "1", Content: "First message", Created: time.Now()},
		{ID: "2", Content: "Second message", Created: time.Now()},
		{ID: "3", Content: "Third message", Created: time.Now()},
	}

	var consumed []TestMessage
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}

		mu.Lock()
		consumed = append(consumed, msg)
		mu.Unlock()

		if len(consumed) >= len(testMessages) {
			cancel()
		}

		return nil
	}

	// Start consumer FIRST
	go func() {
		if err := StartConsumer(ctx, consumerClient, handler); err != nil && err != context.Canceled {
			t.Logf("consumer error: %v", err)
		}
	}()

	// Give consumer time to join group
	time.Sleep(2 * time.Second)

	// THEN produce messages
	for _, msg := range testMessages {
		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
	}

	// Wait for all messages to be consumed
	for len(consumed) < len(testMessages) {
		time.Sleep(500 * time.Millisecond)
		if ctx.Err() != nil {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, len(testMessages), len(consumed), "wrong number of messages consumed")
}

// TestProducerConsumerFlow tests the complete produce-consume flow.
func TestProducerConsumerFlow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	topic := "test-topic-flow"
	logger := slog.New(slog.NewTextHandler(testWriter{t}, nil))

	producerCfg := DefaultConfig(brokers, "").WithLogger(logger)
	producerClient, err := NewKafkaClientFromConfig(ctx, producerCfg)
	require.NoError(t, err)
	defer producerClient.Close()

	producer := NewProducer(producerClient, WithProducerLogger(logger))

	err = ensureTopicExists(ctx, brokers, topic)
	require.NoError(t, err, "failed to create topic")

	consumerCfg := DefaultConfig(brokers, "test-flow-group").
		WithTopics(topic).
		WithLogger(logger)
	consumerClient, err := NewKafkaClientFromConfig(ctx, consumerCfg)
	require.NoError(t, err)
	defer consumerClient.Close()

	expectedCount := 10
	var receivedCount int
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}

		mu.Lock()
		receivedCount++
		t.Logf("Received message %d: %s", receivedCount, msg.ID)
		if receivedCount >= expectedCount {
			cancel()
		}
		mu.Unlock()

		return nil
	}

	go func() {
		_ = StartConsumer(ctx, consumerClient, handler, ConsumerOptions{
			Logger:     logger,
			MaxRetries: 3,
		})
	}()

	time.Sleep(2 * time.Second)

	for i := 0; i < expectedCount; i++ {
		msg := TestMessage{
			ID:      fmt.Sprintf("msg-%d", i),
			Content: fmt.Sprintf("Message number %d", i),
			Created: time.Now(),
		}

		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
		t.Logf("Produced message %d", i)
	}

	<-ctx.Done()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, expectedCount, receivedCount, "not all messages consumed")
}

// TestConsumerWithDLQ tests Dead Letter Queue functionality.
func TestConsumerWithDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	topic := "test-topic-dlq"
	logger := slog.New(slog.NewTextHandler(testWriter{t}, nil))

	producerCfg := DefaultConfig(brokers, "").WithLogger(logger)
	producerClient, err := NewKafkaClientFromConfig(ctx, producerCfg)
	require.NoError(t, err)
	defer producerClient.Close()

	producer := NewProducer(producerClient, WithProducerLogger(logger))

	dlqCfg := DefaultConfig(brokers, "").WithLogger(logger)
	dlqClient, err := NewKafkaClientFromConfig(ctx, dlqCfg)
	require.NoError(t, err)
	defer dlqClient.Close()

	dlqProducer := NewProducer(dlqClient, WithProducerLogger(logger))

	err = ensureTopicExists(ctx, brokers, topic)
	require.NoError(t, err, "failed to create topic")

	dlqTopic := topic + ".dlq"
	err = ensureTopicExists(ctx, brokers, dlqTopic)
	require.NoError(t, err, "failed to create DLQ topic")

	consumerCfg := DefaultConfig(brokers, "test-dlq-group").
		WithTopics(topic).
		WithLogger(logger)
	consumerClient, err := NewKafkaClientFromConfig(ctx, consumerCfg)
	require.NoError(t, err)
	defer consumerClient.Close()

	var dlqCount int
	var mu sync.Mutex

	handler := func(r *kgo.Record) error {
		var msg TestMessage
		if err := json.Unmarshal(r.Value, &msg); err != nil {
			return err
		}

		if msg.Content == "fail" {
			return fmt.Errorf("intentional failure for testing")
		}

		return nil
	}

	go func() {
		_ = StartConsumer(ctx, consumerClient, handler, ConsumerOptions{
			Logger:      logger,
			DLQProducer: dlqProducer,
			MaxRetries:  2,
			OnDLQPublish: func(topic string, err error) {
				mu.Lock()
				dlqCount++
				mu.Unlock()
				t.Logf("Message sent to DLQ: %s - %v", topic, err)
			},
		})
	}()

	time.Sleep(2 * time.Second)

	messages := []TestMessage{
		{ID: "1", Content: "success", Created: time.Now()},
		{ID: "2", Content: "fail", Created: time.Now()},
		{ID: "3", Content: "success", Created: time.Now()},
		{ID: "4", Content: "fail", Created: time.Now()},
	}

	for _, msg := range messages {
		err := producer.SendMessage(ctx, topic, msg)
		require.NoError(t, err)
	}

	time.Sleep(5 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 2, dlqCount, "expected 2 messages in DLQ")
}

// TestHealthChecker tests health check functionality.
func TestHealthChecker(t *testing.T) {
	ctx := context.Background()

	kafkaContainer, brokers := setupKafkaContainer(t, ctx)
	defer kafkaContainer.Terminate(ctx)

	cfg := DefaultConfig(brokers, "test-health-group")
	client, err := NewKafkaClientFromConfig(ctx, cfg)
	require.NoError(t, err)
	defer client.Close()

	checker := NewHealthChecker(client)

	err = checker.Check(ctx)
	assert.NoError(t, err, "health check failed")

	health, err := checker.CheckWithMetadata(ctx)
	assert.NoError(t, err)
	assert.True(t, health.Healthy, "cluster should be healthy")
	assert.Greater(t, health.BrokerCount, 0, "should have at least one broker")
	assert.GreaterOrEqual(t, health.ControllerID, int32(0), "controller ID should be valid")

	err = checker.LivenessCheck(ctx)
	assert.NoError(t, err, "liveness check failed")
}

// TestConfig tests configuration validation.
func TestConfig(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("missing brokers", func(t *testing.T) {
		cfg := DefaultConfig([]string{}, "test-group")
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "broker")
	})

	t.Run("invalid SASL config", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.SASLMechanism = "PLAIN"
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SASL")
	})

	t.Run("unsupported SASL mechanism", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.SASLMechanism = "INVALID"
		cfg.SASLUsername = "user"
		cfg.SASLPassword = "pass"
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported SASL mechanism")
	})

	t.Run("config with TLS", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithTLS(nil)
		assert.True(t, cfg.TLSEnabled)
		assert.NotNil(t, cfg.TLSConfig)
	})

	t.Run("config with SASL PLAIN", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithSASLPlain("user", "pass")
		assert.Equal(t, "PLAIN", cfg.SASLMechanism)
		assert.Equal(t, "user", cfg.SASLUsername)
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("config with SASL SCRAM", func(t *testing.T) {
		cfg := DefaultConfig([]string{"localhost:9092"}, "test-group")
		cfg.WithSASLScram("SCRAM-SHA-256", "user", "pass")
		assert.Equal(t, "SCRAM-SHA-256", cfg.SASLMechanism)
		err := cfg.Validate()
		assert.NoError(t, err)
	})
}

// testWriter wraps testing.T to implement io.Writer for slog.
type testWriter struct {
	t *testing.T
}

func (tw testWriter) Write(p []byte) (n int, err error) {
	tw.t.Log(string(p))
	return len(p), nil
}
