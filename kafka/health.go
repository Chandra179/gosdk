package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ClusterHealth contains health information about the Kafka cluster.
type ClusterHealth struct {
	Healthy      bool
	BrokerCount  int
	ControllerID int32
	ClusterID    string
	ResponseTime time.Duration
	TopicCount   int
	ErrorMessage string
}

// HealthChecker provides health check functionality for Kafka.
type HealthChecker struct {
	client *kgo.Client
}

// NewHealthChecker creates a new HealthChecker.
func NewHealthChecker(client *kgo.Client) *HealthChecker {
	return &HealthChecker{client: client}
}

// Check performs a basic health check by pinging the cluster.
// Returns nil if the cluster is reachable and healthy.
func (h *HealthChecker) Check(ctx context.Context) error {
	health, err := h.CheckWithMetadata(ctx)
	if err != nil {
		return err
	}
	if !health.Healthy {
		return fmt.Errorf("kafka cluster unhealthy: %s", health.ErrorMessage)
	}
	return nil
}

// CheckWithMetadata performs a health check and returns detailed cluster information.
func (h *HealthChecker) CheckWithMetadata(ctx context.Context) (*ClusterHealth, error) {
	start := time.Now()
	health := &ClusterHealth{}

	adminClient := kadm.NewClient(h.client)

	// Get cluster metadata
	metadata, err := adminClient.Metadata(ctx)
	if err != nil {
		health.Healthy = false
		health.ErrorMessage = fmt.Sprintf("failed to get metadata: %v", err)
		health.ResponseTime = time.Since(start)
		return health, nil
	}

	health.ResponseTime = time.Since(start)
	health.ClusterID = metadata.Cluster
	health.ControllerID = metadata.Controller
	health.BrokerCount = len(metadata.Brokers)
	health.TopicCount = len(metadata.Topics)

	// Check if we have at least one broker
	if health.BrokerCount == 0 {
		health.Healthy = false
		health.ErrorMessage = "no brokers available"
		return health, nil
	}

	// Check if controller is valid
	if health.ControllerID < 0 {
		health.Healthy = false
		health.ErrorMessage = "no controller elected"
		return health, nil
	}

	health.Healthy = true
	return health, nil
}

// LivenessCheck is a convenience function for Kubernetes liveness probes.
// It performs a lightweight check to verify the client is still connected.
func (h *HealthChecker) LivenessCheck(ctx context.Context) error {
	// Use a short timeout for liveness checks
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return h.Check(ctx)
}
