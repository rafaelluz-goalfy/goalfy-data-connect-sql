package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/goalfy/goalfy-data-connect-sql/internal/config"
	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// Publisher publishes sync lifecycle events to Kafka
type Publisher struct {
	writer *kafkago.Writer
	cfg    *config.KafkaConfig
}

// NewPublisher creates a new Publisher
func NewPublisher(cfg *config.KafkaConfig) *Publisher {
	w := &kafkago.Writer{
		Addr:         kafkago.TCP(cfg.Brokers...),
		Balancer:     &kafkago.LeastBytes{},
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
	}
	return &Publisher{writer: w, cfg: cfg}
}

// Close shuts down the Kafka writer
func (p *Publisher) Close() error {
	return p.writer.Close()
}

// syncStartedPayload is the payload expected by the orchestrator's sync.started consumer.
type syncStartedPayload struct {
	ExecutionID string    `json:"execution_id"`
	Timestamp   time.Time `json:"timestamp"`
}

// syncCompletedPayload is the payload expected by the orchestrator's sync.completed consumer.
type syncCompletedPayload struct {
	ExecutionID string    `json:"execution_id"`
	Timestamp   time.Time `json:"timestamp"`
}

// syncFailedPayload is the payload expected by the orchestrator's sync.failed consumer.
type syncFailedPayload struct {
	ExecutionID  string    `json:"execution_id"`
	Timestamp    time.Time `json:"timestamp"`
	ErrorMessage string    `json:"error_message"`
}

// SyncStarted publishes data.connect.sync.started with the minimal payload the orchestrator expects.
func (p *Publisher) SyncStarted(ctx context.Context, exec *models.ExecutionContext) error {
	payload := syncStartedPayload{
		ExecutionID: exec.ExecutionID,
		Timestamp:   time.Now().UTC(),
	}
	return p.publishPayload(ctx, p.cfg.TopicStarted, exec.ExecutionID, payload)
}

// BatchProcessed publishes data.connect.batch.processed
func (p *Publisher) BatchProcessed(ctx context.Context, exec *models.ExecutionContext, batchNum, count int) error {
	extra := map[string]interface{}{
		"batch_number":     batchNum,
		"records_in_batch": count,
	}
	return p.publish(ctx, p.cfg.TopicBatch, "data.connect.batch.processed", exec, extra)
}

// SyncCompleted publishes data.connect.sync.completed with the minimal payload the orchestrator expects.
func (p *Publisher) SyncCompleted(ctx context.Context, exec *models.ExecutionContext, result *models.ExecutionResult) error {
	payload := syncCompletedPayload{
		ExecutionID: exec.ExecutionID,
		Timestamp:   time.Now().UTC(),
	}
	return p.publishPayload(ctx, p.cfg.TopicCompleted, exec.ExecutionID, payload)
}

// SyncFailed publishes data.connect.sync.failed with the minimal payload the orchestrator expects.
func (p *Publisher) SyncFailed(ctx context.Context, exec *models.ExecutionContext, syncErr error) error {
	payload := syncFailedPayload{
		ExecutionID:  exec.ExecutionID,
		Timestamp:    time.Now().UTC(),
		ErrorMessage: syncErr.Error(),
	}
	return p.publishPayload(ctx, p.cfg.TopicFailed, exec.ExecutionID, payload)
}

// --- internal ---

type eventPayload struct {
	EventType     string                 `json:"event_type"`
	TenantID      string                 `json:"tenant_id"`
	SourceID      string                 `json:"source_id"`
	DatasetID     string                 `json:"dataset_id"`
	ExecutionID   string                 `json:"execution_id"`
	ConnectorType string                 `json:"connector_type"`
	TriggerType   string                 `json:"trigger_type"`
	CorrelationID string                 `json:"correlation_id"`
	Timestamp     time.Time              `json:"timestamp"`
	Extra         map[string]interface{} `json:"extra,omitempty"`
}

// publishPayload serialises any struct and writes it to the given topic.
// key is used as the Kafka message key for partition routing.
func (p *Publisher) publishPayload(ctx context.Context, topic, key string, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: b,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing kafka message to %s: %w", topic, err)
	}
	return nil
}

func (p *Publisher) publish(ctx context.Context, topic, eventType string, exec *models.ExecutionContext, extra map[string]interface{}) error {
	payload := eventPayload{
		EventType:     eventType,
		TenantID:      exec.TenantID,
		SourceID:      exec.SourceID,
		DatasetID:     exec.DatasetID,
		ExecutionID:   exec.ExecutionID,
		ConnectorType: string(exec.ConnectorType),
		TriggerType:   string(exec.TriggerType),
		CorrelationID: exec.CorrelationID,
		Timestamp:     time.Now().UTC(),
		Extra:         extra,
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(exec.ExecutionID),
		Value: b,
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing kafka message to %s: %w", topic, err)
	}
	return nil
}

// Consumer reads events from the sync.requested topic
type Consumer struct {
	reader *kafkago.Reader
}

// newTopicReader creates a kafka-go reader for a specific topic
func newTopicReader(cfg *config.KafkaConfig, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          topic,
		MaxBytes:       cfg.MaxBytes,
		CommitInterval: cfg.CommitInterval,
	})
}

// NewConsumer creates a Consumer for data.connect.sync.requested events
func NewConsumer(cfg *config.KafkaConfig) *Consumer {
	return &Consumer{reader: newTopicReader(cfg, cfg.TopicSyncReq)}
}

// NewTestConsumer creates a Consumer for data.connect.test.requested events
func NewTestConsumer(cfg *config.KafkaConfig) *Consumer {
	return &Consumer{reader: newTopicReader(cfg, cfg.TopicTestReq)}
}

// NewDiscoveryConsumer creates a Consumer for data.connect.discovery.requested events
func NewDiscoveryConsumer(cfg *config.KafkaConfig) *Consumer {
	return &Consumer{reader: newTopicReader(cfg, cfg.TopicDiscoveryReq)}
}

// NewCheckpointResetConsumer creates a Consumer for data.connect.checkpoint.reset events
func NewCheckpointResetConsumer(cfg *config.KafkaConfig) *Consumer {
	return &Consumer{reader: newTopicReader(cfg, cfg.TopicCheckpointReset)}
}

// Close shuts down the Kafka reader
func (c *Consumer) Close() error {
	return c.reader.Close()
}

// ReadSyncEvent blocks until a SyncEvent is available, then returns it
func (c *Consumer) ReadSyncEvent(ctx context.Context) (*models.SyncEvent, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading kafka message: %w", err)
	}

	var event models.SyncEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, fmt.Errorf("unmarshaling sync event: %w", err)
	}
	return &event, nil
}

// ReadTestConnectionEvent blocks until a TestConnectionEvent is available
func (c *Consumer) ReadTestConnectionEvent(ctx context.Context) (*models.TestConnectionEvent, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading kafka message: %w", err)
	}
	var event models.TestConnectionEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, fmt.Errorf("unmarshaling test connection event: %w", err)
	}
	return &event, nil
}

// ReadDiscoveryEvent blocks until a DiscoveryEvent is available
func (c *Consumer) ReadDiscoveryEvent(ctx context.Context) (*models.DiscoveryEvent, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading kafka message: %w", err)
	}
	var event models.DiscoveryEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, fmt.Errorf("unmarshaling discovery event: %w", err)
	}
	return &event, nil
}

// ReadCheckpointResetEvent blocks until a CheckpointResetEvent is available
func (c *Consumer) ReadCheckpointResetEvent(ctx context.Context) (*models.CheckpointResetEvent, error) {
	msg, err := c.reader.ReadMessage(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading kafka message: %w", err)
	}
	var event models.CheckpointResetEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return nil, fmt.Errorf("unmarshaling checkpoint reset event: %w", err)
	}
	return &event, nil
}

// PublishTestConnectionResult publishes a connection test result to data.connect.test.result
func (p *Publisher) PublishTestConnectionResult(ctx context.Context, result *models.TestConnectionResult) error {
	b, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal test connection result: %w", err)
	}
	msg := kafkago.Message{
		Topic: p.cfg.TopicTestResult,
		Key:   []byte(result.RequestID),
		Value: b,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing test connection result to %s: %w", p.cfg.TopicTestResult, err)
	}
	return nil
}

// PublishDiscoveryResult publishes a discovery summary to data.connect.discovery.result
func (p *Publisher) PublishDiscoveryResult(ctx context.Context, result *models.DiscoveryResult) error {
	b, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal discovery result: %w", err)
	}
	msg := kafkago.Message{
		Topic: p.cfg.TopicDiscoveryResult,
		Key:   []byte(result.RequestID),
		Value: b,
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("writing discovery result to %s: %w", p.cfg.TopicDiscoveryResult, err)
	}
	return nil
}
