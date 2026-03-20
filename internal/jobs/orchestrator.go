package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/goalfy/goalfy-data-connect-sql/internal/checkpoint"
	sqlconn "github.com/goalfy/goalfy-data-connect-sql/internal/connectors/sql"
	"github.com/goalfy/goalfy-data-connect-sql/internal/discovery"
	"github.com/goalfy/goalfy-data-connect-sql/internal/kafka"
	"github.com/goalfy/goalfy-data-connect-sql/internal/locks"
	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
	"github.com/goalfy/goalfy-data-connect-sql/internal/persistence"
	"github.com/goalfy/goalfy-data-connect-sql/internal/reader"
)

// ConfigLoader fetches and persists source and dataset config in the internal DB
type ConfigLoader interface {
	LoadSource(ctx context.Context, tenantID, sourceID string) (*models.SourceConfig, error)
	LoadDataset(ctx context.Context, tenantID, sourceID, datasetID string) (*models.DatasetConfig, error)
	UpsertSource(ctx context.Context, source *models.SourceConfig) error
	UpsertDataset(ctx context.Context, dataset *models.DatasetConfig) error
}

// Orchestrator coordinates a full sync execution
type Orchestrator struct {
	configLoader  ConfigLoader
	checkpointMgr *checkpoint.Manager
	lockMgr       *locks.Manager
	publisher     *kafka.Publisher
	writer        *persistence.Writer
	secretLoader  interface{ Resolve(string) (string, error) }
	logger        *zap.Logger
}

// NewOrchestrator creates a new Orchestrator
func NewOrchestrator(
	configLoader ConfigLoader,
	checkpointMgr *checkpoint.Manager,
	lockMgr *locks.Manager,
	publisher *kafka.Publisher,
	writer *persistence.Writer,
	secretLoader interface{ Resolve(string) (string, error) },
	logger *zap.Logger,
) *Orchestrator {
	return &Orchestrator{
		configLoader:  configLoader,
		checkpointMgr: checkpointMgr,
		lockMgr:       lockMgr,
		publisher:     publisher,
		writer:        writer,
		secretLoader:  secretLoader,
		logger:        logger,
	}
}

// Execute runs a full sync for the given event
func (o *Orchestrator) Execute(ctx context.Context, event *models.SyncEvent) error {
	log := o.logger.With(
		zap.String("execution_id", event.ExecutionID),
		zap.String("tenant_id", event.TenantID),
		zap.String("source_id", event.SourceID),
		zap.String("dataset_id", event.DatasetID),
	)

	log.Info("sync execution started")

	// 1. Acquire distributed lock
	release, err := o.lockMgr.Acquire(ctx, event.TenantID, event.SourceID, event.DatasetID)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer release()

	// 2. Load source and dataset config
	source, err := o.configLoader.LoadSource(ctx, event.TenantID, event.SourceID)
	if err != nil {
		return fmt.Errorf("load source config: %w", err)
	}

	dataset, err := o.configLoader.LoadDataset(ctx, event.TenantID, event.SourceID, event.DatasetID)
	if err != nil {
		return fmt.Errorf("load dataset config: %w", err)
	}

	if !dataset.Enabled {
		log.Info("dataset is disabled, skipping sync")
		return nil
	}

	// 3. Resolve password from secret
	password, err := o.secretLoader.Resolve(source.PasswordRef)
	if err != nil {
		return fmt.Errorf("resolve password: %w", err)
	}

	exec := &models.ExecutionContext{
		TenantID:      event.TenantID,
		SourceID:      event.SourceID,
		DatasetID:     event.DatasetID,
		ExecutionID:   event.ExecutionID,
		ConnectorType: event.ConnectorType,
		TriggerType:   event.TriggerType,
		CorrelationID: event.CorrelationID,
		StartedAt:     time.Now().UTC(),
		Source:        source,
		Dataset:       dataset,
		Password:      password,
	}

	// 4. Publish started event
	if err := o.publisher.SyncStarted(ctx, exec); err != nil {
		log.Warn("failed to publish sync.started event", zap.Error(err))
	}

	// 5. Execute sync
	result, syncErr := o.runSync(ctx, exec, log)

	// 6. Record job status
	result.CompletedAt = time.Now().UTC()
	if syncErr != nil {
		result.Status = models.JobStatusFailed
		result.Error = syncErr.Error()
		_ = o.publisher.SyncFailed(ctx, exec, syncErr)
		log.Error("sync execution failed", zap.Error(syncErr))
	} else {
		result.Status = models.JobStatusCompleted
		_ = o.publisher.SyncCompleted(ctx, exec, result)
		log.Info("sync execution completed",
			zap.Int("total_records", result.TotalRecords),
			zap.Int("total_persisted", result.TotalPersisted),
			zap.Int("total_batches", result.TotalBatches),
			zap.Duration("duration", result.Duration),
		)
	}

	if err := o.writer.UpdateJobStatus(ctx, exec, result); err != nil {
		log.Warn("failed to update job status", zap.Error(err))
	}

	return syncErr
}

func (o *Orchestrator) runSync(ctx context.Context, exec *models.ExecutionContext, log *zap.Logger) (*models.ExecutionResult, error) {
	result := &models.ExecutionResult{ExecutionID: exec.ExecutionID}
	start := time.Now()
	defer func() { result.Duration = time.Since(start) }()

	// Open connection to external DB
	connector, err := sqlconn.NewConnector(exec.Source, exec.Password)
	if err != nil {
		return result, fmt.Errorf("create connector: %w", err)
	}

	connCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := connector.Connect(connCtx); err != nil {
		return result, fmt.Errorf("connect to source: %w", err)
	}
	defer connector.Close()

	if err := connector.Ping(connCtx); err != nil {
		return result, fmt.Errorf("ping source: %w", err)
	}

	db := connector.DB()
	r := reader.New(db, exec.ConnectorType, log)

	totalRecords := 0
	totalPersisted := 0
	batchNum := 0

	batchHandler := func(batchCtx context.Context, batch []map[string]interface{}, num int) error {
		batchNum = num
		log.Info("processing batch",
			zap.Int("batch", num),
			zap.Int("records", len(batch)),
		)

		if err := o.writer.WriteRawBatch(batchCtx, exec, batch); err != nil {
			return fmt.Errorf("write raw batch %d: %w", num, err)
		}
		if err := o.writer.WriteNormalizedBatch(batchCtx, exec, batch); err != nil {
			return fmt.Errorf("write normalized batch %d: %w", num, err)
		}

		totalRecords += len(batch)
		totalPersisted += len(batch)

		// Refresh lock to prevent expiry during long processing
		_ = o.lockMgr.Refresh(ctx, exec.TenantID, exec.SourceID, exec.DatasetID)

		_ = o.publisher.BatchProcessed(ctx, exec, num, len(batch))

		return nil
	}

	switch exec.Dataset.SyncMode {
	case models.SyncModeFull:
		n, err := r.ReadFull(ctx, exec.Dataset, batchHandler)
		if err != nil {
			return result, err
		}
		totalRecords = n

		// Clear checkpoint after full reload
		if err := o.checkpointMgr.Delete(ctx, exec.TenantID, exec.SourceID, exec.DatasetID); err != nil {
			log.Warn("failed to delete checkpoint after full load", zap.Error(err))
		}

	case models.SyncModeIncremental:
		cp, err := o.checkpointMgr.Load(ctx, exec.TenantID, exec.SourceID, exec.DatasetID)
		if err != nil {
			return result, fmt.Errorf("load checkpoint: %w", err)
		}

		lastValue := ""
		if cp != nil {
			lastValue = cp.LastValue
		}

		n, newLastValue, err := r.ReadIncremental(ctx, exec.Dataset, lastValue, batchHandler)
		if err != nil {
			return result, err
		}
		totalRecords = n

		if newLastValue != "" && newLastValue != lastValue {
			if err := o.checkpointMgr.Save(ctx, &models.Checkpoint{
				TenantID:  exec.TenantID,
				SourceID:  exec.SourceID,
				DatasetID: exec.DatasetID,
				LastValue: newLastValue,
			}); err != nil {
				log.Warn("failed to save checkpoint", zap.Error(err))
			}
		}

	default:
		return result, fmt.Errorf("unsupported sync mode: %s", exec.Dataset.SyncMode)
	}

	result.TotalRecords = totalRecords
	result.TotalPersisted = totalPersisted
	result.TotalBatches = batchNum

	return result, nil
}

// TestConnectionByID loads source config from DB by ID and runs a connection test.
// Used by the Kafka consumer path where only tenant/source IDs are available.
func (o *Orchestrator) TestConnectionByID(ctx context.Context, tenantID, sourceID, requestID string) (*models.TestConnectionResult, error) {
	source, err := o.configLoader.LoadSource(ctx, tenantID, sourceID)
	if err != nil {
		return nil, fmt.Errorf("load source: %w", err)
	}
	return o.TestConnection(ctx, source, requestID)
}

// TestConnection tests connectivity using an inline SourceConfig, without any DB lookup or persistence.
func (o *Orchestrator) TestConnection(ctx context.Context, source *models.SourceConfig, requestID string) (*models.TestConnectionResult, error) {
	password, err := o.secretLoader.Resolve(source.PasswordRef)
	if err != nil {
		return nil, fmt.Errorf("resolve password: %w", err)
	}

	connector, err := sqlconn.NewConnector(source, password)
	if err != nil {
		return nil, err
	}

	res, err := connector.TestConnection(ctx)
	if err != nil {
		return nil, err
	}

	return &models.TestConnectionResult{
		RequestID:      requestID,
		TenantID:       source.TenantID,
		SourceID:       source.SourceID,
		Success:        res.Success,
		Message:        res.Message,
		ResponseTimeMs: res.ResponseTime.Milliseconds(),
		Error:          res.Error,
		Timestamp:      time.Now().UTC(),
	}, nil
}

// DiscoverSchemaByID loads source config from DB by ID and runs schema discovery.
// Used by the Kafka consumer path where only tenant/source IDs are available.
func (o *Orchestrator) DiscoverSchemaByID(ctx context.Context, tenantID, sourceID, filterSchema string, allowedTables []string) (*models.SchemaDiscovery, error) {
	source, err := o.configLoader.LoadSource(ctx, tenantID, sourceID)
	if err != nil {
		return nil, fmt.Errorf("load source: %w", err)
	}
	return o.DiscoverSchema(ctx, source, filterSchema, allowedTables)
}

// DiscoverSchema runs schema discovery using an inline SourceConfig, without any DB lookup or persistence.
func (o *Orchestrator) DiscoverSchema(ctx context.Context, source *models.SourceConfig, filterSchema string, allowedTables []string) (*models.SchemaDiscovery, error) {
	password, err := o.secretLoader.Resolve(source.PasswordRef)
	if err != nil {
		return nil, fmt.Errorf("resolve password: %w", err)
	}

	connector, err := sqlconn.NewConnector(source, password)
	if err != nil {
		return nil, err
	}

	if err := connector.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer connector.Close()

	d := discovery.New(connector.DB(), source.ConnectorType)
	return d.Discover(ctx, source.TenantID, source.SourceID, filterSchema, allowedTables)
}

// ResetCheckpoint deletes the sync checkpoint for a dataset, enabling controlled reprocessing
func (o *Orchestrator) ResetCheckpoint(ctx context.Context, tenantID, sourceID, datasetID string) error {
	return o.checkpointMgr.Delete(ctx, tenantID, sourceID, datasetID)
}

// UpsertSource creates or updates a SourceConfig in the internal database
func (o *Orchestrator) UpsertSource(ctx context.Context, source *models.SourceConfig) error {
	return o.configLoader.UpsertSource(ctx, source)
}

// UpsertDataset creates or updates a DatasetConfig in the internal database
func (o *Orchestrator) UpsertDataset(ctx context.Context, dataset *models.DatasetConfig) error {
	return o.configLoader.UpsertDataset(ctx, dataset)
}

// DBConfigLoader is a database-backed ConfigLoader
type DBConfigLoader struct {
	db *sql.DB
}

// NewDBConfigLoader creates a new DBConfigLoader
func NewDBConfigLoader(db *sql.DB) *DBConfigLoader {
	return &DBConfigLoader{db: db}
}

func (l *DBConfigLoader) LoadSource(ctx context.Context, tenantID, sourceID string) (*models.SourceConfig, error) {
	const q = `
		SELECT tenant_id, source_id, connector_type, host, port, database_name,
			   schema_name, username, password_ref, ssl_mode
		FROM data_sources
		WHERE tenant_id = $1 AND source_id = $2
		LIMIT 1`

	row := l.db.QueryRowContext(ctx, q, tenantID, sourceID)
	s := &models.SourceConfig{}
	var schemaName *string
	err := row.Scan(
		&s.TenantID, &s.SourceID, &s.ConnectorType,
		&s.Host, &s.Port, &s.Database, &schemaName,
		&s.Username, &s.PasswordRef, &s.SSLMode,
	)
	if err != nil {
		return nil, fmt.Errorf("load source %s/%s: %w", tenantID, sourceID, err)
	}
	if schemaName != nil {
		s.Schema = *schemaName
	}
	return s, nil
}

func (l *DBConfigLoader) LoadDataset(ctx context.Context, tenantID, sourceID, datasetID string) (*models.DatasetConfig, error) {
	const q = `
		SELECT tenant_id, source_id, dataset_id, schema_name, table_name, object_type,
			   sync_mode, incremental_column, incremental_type, technical_key, batch_size, enabled
		FROM data_datasets
		WHERE tenant_id = $1 AND source_id = $2 AND dataset_id = $3
		LIMIT 1`

	row := l.db.QueryRowContext(ctx, q, tenantID, sourceID, datasetID)
	d := &models.DatasetConfig{}

	var incrementalColumn, incrementalType, technicalKey, schemaName *string

	err := row.Scan(
		&d.TenantID, &d.SourceID, &d.DatasetID,
		&schemaName, &d.TableName, &d.ObjectType,
		&d.SyncMode, &incrementalColumn, &incrementalType,
		&technicalKey, &d.BatchSize, &d.Enabled,
	)
	if err != nil {
		return nil, fmt.Errorf("load dataset %s/%s/%s: %w", tenantID, sourceID, datasetID, err)
	}

	if schemaName != nil {
		d.SchemaName = *schemaName
	}
	if incrementalColumn != nil {
		d.IncrementalColumn = *incrementalColumn
	}
	if incrementalType != nil {
		d.IncrementalType = models.IncrementalType(*incrementalType)
	}
	if technicalKey != nil {
		d.TechnicalKey = *technicalKey
	}

	return d, nil
}

func (l *DBConfigLoader) UpsertSource(ctx context.Context, source *models.SourceConfig) error {
	_, err := l.db.ExecContext(ctx, `
		INSERT INTO data_sources
			(tenant_id, source_id, connector_type, host, port, database_name,
			 schema_name, username, password_ref, ssl_mode, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, now(), now())
		ON CONFLICT (tenant_id, source_id)
		DO UPDATE SET
			connector_type = EXCLUDED.connector_type,
			host           = EXCLUDED.host,
			port           = EXCLUDED.port,
			database_name  = EXCLUDED.database_name,
			schema_name    = EXCLUDED.schema_name,
			username       = EXCLUDED.username,
			password_ref   = EXCLUDED.password_ref,
			ssl_mode       = EXCLUDED.ssl_mode,
			updated_at     = now()`,
		source.TenantID, source.SourceID, string(source.ConnectorType),
		source.Host, source.Port, source.Database,
		source.Schema, source.Username, source.PasswordRef, source.SSLMode,
	)
	return err
}

func (l *DBConfigLoader) UpsertDataset(ctx context.Context, dataset *models.DatasetConfig) error {
	_, err := l.db.ExecContext(ctx, `
		INSERT INTO data_datasets
			(tenant_id, source_id, dataset_id, schema_name, table_name, object_type,
			 sync_mode, incremental_column, incremental_type, technical_key,
			 batch_size, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, NULLIF($4, ''), $5, $6, $7,
		        NULLIF($8, ''), NULLIF($9, ''), NULLIF($10, ''),
		        $11, $12, now(), now())
		ON CONFLICT (tenant_id, source_id, dataset_id)
		DO UPDATE SET
			schema_name        = EXCLUDED.schema_name,
			table_name         = EXCLUDED.table_name,
			object_type        = EXCLUDED.object_type,
			sync_mode          = EXCLUDED.sync_mode,
			incremental_column = EXCLUDED.incremental_column,
			incremental_type   = EXCLUDED.incremental_type,
			technical_key      = EXCLUDED.technical_key,
			batch_size         = EXCLUDED.batch_size,
			enabled            = EXCLUDED.enabled,
			updated_at         = now()`,
		dataset.TenantID, dataset.SourceID, dataset.DatasetID,
		dataset.SchemaName, dataset.TableName, dataset.ObjectType,
		string(dataset.SyncMode),
		dataset.IncrementalColumn, string(dataset.IncrementalType), dataset.TechnicalKey,
		dataset.BatchSize, dataset.Enabled,
	)
	return err
}
