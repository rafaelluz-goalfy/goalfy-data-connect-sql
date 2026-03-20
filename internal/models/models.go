package models

import "time"

// ConnectorType represents supported SQL databases
type ConnectorType string

const (
	ConnectorPostgres  ConnectorType = "postgres"
	ConnectorMySQL     ConnectorType = "mysql"
	ConnectorSQLServer ConnectorType = "sqlserver"
)

// SyncMode defines the synchronization strategy
type SyncMode string

const (
	SyncModeFull        SyncMode = "full"
	SyncModeIncremental SyncMode = "incremental"
)

// IncrementalType defines the incremental strategy
type IncrementalType string

const (
	IncrementalByDate IncrementalType = "date"
	IncrementalByKey  IncrementalType = "key"
)

// TriggerType defines what triggered the sync
type TriggerType string

const (
	TriggerManual    TriggerType = "manual"
	TriggerScheduled TriggerType = "scheduled"
	TriggerEvent     TriggerType = "event"
)

// JobStatus represents the execution status
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusCompleted JobStatus = "completed"
	JobStatusFailed    JobStatus = "failed"
)

// SourceConfig holds connection details for an external SQL source
type SourceConfig struct {
	TenantID      string        `json:"tenant_id" db:"tenant_id"`
	SourceID      string        `json:"source_id" db:"source_id"`
	ConnectorType ConnectorType `json:"connector_type" db:"connector_type"`
	Host          string        `json:"host" db:"host"`
	Port          int           `json:"port" db:"port"`
	Database      string        `json:"database" db:"database"`
	Schema        string        `json:"schema" db:"schema"`
	Username      string        `json:"username" db:"username"`
	PasswordRef   string        `json:"password_ref" db:"password_ref"` // reference to secret, never plain text
	SSLMode       string        `json:"ssl_mode" db:"ssl_mode"`
}

// DatasetConfig holds per-dataset sync configuration
type DatasetConfig struct {
	TenantID          string          `json:"tenant_id" db:"tenant_id"`
	SourceID          string          `json:"source_id" db:"source_id"`
	DatasetID         string          `json:"dataset_id" db:"dataset_id"`
	SchemaName        string          `json:"schema_name" db:"schema_name"`
	TableName         string          `json:"table_name" db:"table_name"`
	ObjectType        string          `json:"object_type" db:"object_type"` // table | view
	SyncMode          SyncMode        `json:"sync_mode" db:"sync_mode"`
	IncrementalColumn string          `json:"incremental_column" db:"incremental_column"`
	IncrementalType   IncrementalType `json:"incremental_type" db:"incremental_type"`
	TechnicalKey      string          `json:"technical_key" db:"technical_key"`
	BatchSize         int             `json:"batch_size" db:"batch_size"`
	Enabled           bool            `json:"enabled" db:"enabled"`
}

// SyncEvent is the Kafka event received to trigger a sync
type SyncEvent struct {
	TenantID      string        `json:"tenant_id"`
	SourceID      string        `json:"source_id"`
	DatasetID     string        `json:"dataset_id"`
	ExecutionID   string        `json:"execution_id"`
	ConnectorType ConnectorType `json:"connector_type"`
	TriggerType   TriggerType   `json:"trigger_type"`
	CorrelationID string        `json:"correlation_id"`
	Timestamp     time.Time     `json:"timestamp"`
}

// ExecutionContext holds runtime state for a sync execution
type ExecutionContext struct {
	TenantID      string
	SourceID      string
	DatasetID     string
	ExecutionID   string
	ConnectorType ConnectorType
	TriggerType   TriggerType
	CorrelationID string
	StartedAt     time.Time
	Source        *SourceConfig
	Dataset       *DatasetConfig
	Password      string // loaded from secret, never persisted
}

// ColumnInfo describes a column from schema discovery
type ColumnInfo struct {
	Name       string `json:"name"`
	DataType   string `json:"data_type"`
	Nullable   bool   `json:"nullable"`
	IsPrimary  bool   `json:"is_primary"`
	OrdinalPos int    `json:"ordinal_position"`
}

// TableInfo describes a table or view from schema discovery
type TableInfo struct {
	SchemaName string       `json:"schema_name"`
	TableName  string       `json:"table_name"`
	ObjectType string       `json:"object_type"` // table | view
	Columns    []ColumnInfo `json:"columns"`
}

// SchemaDiscovery holds the full discovery result
type SchemaDiscovery struct {
	TenantID     string      `json:"tenant_id"`
	SourceID     string      `json:"source_id"`
	Schemas      []string    `json:"schemas"`
	Tables       []TableInfo `json:"tables"`
	DiscoveredAt time.Time   `json:"discovered_at"`
}

// Checkpoint holds the last sync watermark for a dataset
type Checkpoint struct {
	TenantID  string    `json:"tenant_id" db:"tenant_id"`
	SourceID  string    `json:"source_id" db:"source_id"`
	DatasetID string    `json:"dataset_id" db:"dataset_id"`
	LastValue string    `json:"last_value" db:"last_value"` // stores date string or ID string
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
}

// BatchResult holds the result of processing a single batch
type BatchResult struct {
	BatchNumber      int
	RecordsRead      int
	RecordsPersisted int
	StartedAt        time.Time
	FinishedAt       time.Time
}

// ExecutionResult holds the final result of a sync execution
type ExecutionResult struct {
	ExecutionID    string
	Status         JobStatus
	TotalRecords   int
	TotalPersisted int
	TotalBatches   int
	Duration       time.Duration
	Error          string
	CompletedAt    time.Time
}

// RawIngestionRecord represents a row stored in raw_ingestion
type RawIngestionRecord struct {
	TenantID    string                 `db:"tenant_id"`
	SourceID    string                 `db:"source_id"`
	DatasetID   string                 `db:"dataset_id"`
	ExecutionID string                 `db:"execution_id"`
	RecordKey   string                 `db:"record_key"`
	Payload     map[string]interface{} `db:"payload"`
	IngestedAt  time.Time              `db:"ingested_at"`
}

// NormalizedRecord represents a technically normalized row
type NormalizedRecord struct {
	TenantID     string                 `db:"tenant_id"`
	SourceID     string                 `db:"source_id"`
	DatasetID    string                 `db:"dataset_id"`
	ExecutionID  string                 `db:"execution_id"`
	RecordKey    string                 `db:"record_key"`
	Payload      map[string]interface{} `db:"payload"`
	NormalizedAt time.Time              `db:"normalized_at"`
}

// TestConnectionEvent triggers a connection test for a registered source
type TestConnectionEvent struct {
	TenantID  string `json:"tenant_id"`
	SourceID  string `json:"source_id"`
	RequestID string `json:"request_id"`
}

// TestConnectionResult is the result of a connection test published to Kafka
type TestConnectionResult struct {
	RequestID      string    `json:"request_id"`
	TenantID       string    `json:"tenant_id"`
	SourceID       string    `json:"source_id"`
	Success        bool      `json:"success"`
	Message        string    `json:"message"`
	ResponseTimeMs int64     `json:"response_time_ms"`
	Error          string    `json:"error,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
}

// DiscoveryEvent triggers schema discovery for a registered source
type DiscoveryEvent struct {
	TenantID      string   `json:"tenant_id"`
	SourceID      string   `json:"source_id"`
	RequestID     string   `json:"request_id"`
	FilterSchema  string   `json:"filter_schema,omitempty"`
	AllowedTables []string `json:"allowed_tables,omitempty"`
}

// DiscoveryResult is a summary of a schema discovery published to Kafka
type DiscoveryResult struct {
	RequestID   string    `json:"request_id"`
	TenantID    string    `json:"tenant_id"`
	SourceID    string    `json:"source_id"`
	Success     bool      `json:"success"`
	SchemaCount int       `json:"schema_count"`
	TableCount  int       `json:"table_count"`
	Error       string    `json:"error,omitempty"`
	Timestamp   time.Time `json:"timestamp"`
}

// CheckpointResetEvent triggers checkpoint deletion to allow controlled reprocessing
type CheckpointResetEvent struct {
	TenantID  string `json:"tenant_id"`
	SourceID  string `json:"source_id"`
	DatasetID string `json:"dataset_id"`
	RequestID string `json:"request_id"`
}
