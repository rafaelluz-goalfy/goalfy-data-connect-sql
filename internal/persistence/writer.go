package persistence

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// Writer persists records to the internal platform database
type Writer struct {
	db *sql.DB
}

// New creates a new Writer
func New(db *sql.DB) *Writer {
	return &Writer{db: db}
}

// WriteRawBatch persists a batch of raw records idempotently
func (w *Writer) WriteRawBatch(ctx context.Context, exec *models.ExecutionContext, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx raw: %w", err)
	}
	defer tx.Rollback()

	for _, row := range batch {
		key := buildRecordKey(exec.Dataset.TechnicalKey, row)
		payload, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshaling raw row: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO raw_ingestion
				(tenant_id, source_id, dataset_id, execution_id, record_key, payload, ingested_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (tenant_id, source_id, dataset_id, record_key)
			DO UPDATE SET
				payload      = EXCLUDED.payload,
				execution_id = EXCLUDED.execution_id,
				ingested_at  = EXCLUDED.ingested_at`,
			exec.TenantID, exec.SourceID, exec.DatasetID, exec.ExecutionID,
			key, payload, time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("upsert raw_ingestion: %w", err)
		}
	}

	return tx.Commit()
}

// WriteNormalizedBatch persists a batch of normalized records idempotently
func (w *Writer) WriteNormalizedBatch(ctx context.Context, exec *models.ExecutionContext, batch []map[string]interface{}) error {
	if len(batch) == 0 {
		return nil
	}

	normalized := normalizeBatch(batch)

	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx normalized: %w", err)
	}
	defer tx.Rollback()

	for _, row := range normalized {
		key := buildRecordKey(exec.Dataset.TechnicalKey, row)
		payload, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshaling normalized row: %w", err)
		}

		_, err = tx.ExecContext(ctx, `
			INSERT INTO normalized
				(tenant_id, source_id, dataset_id, execution_id, record_key, payload, normalized_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (tenant_id, source_id, dataset_id, record_key)
			DO UPDATE SET
				payload       = EXCLUDED.payload,
				execution_id  = EXCLUDED.execution_id,
				normalized_at = EXCLUDED.normalized_at`,
			exec.TenantID, exec.SourceID, exec.DatasetID, exec.ExecutionID,
			key, payload, time.Now().UTC(),
		)
		if err != nil {
			return fmt.Errorf("upsert normalized: %w", err)
		}
	}

	return tx.Commit()
}

// --- internal helpers ---

// normalizeBatch converts dates to UTC, trims string whitespace, and cleans types
func normalizeBatch(batch []map[string]interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, len(batch))
	for i, row := range batch {
		norm := make(map[string]interface{}, len(row))
		for k, v := range row {
			norm[strings.ToLower(strings.TrimSpace(k))] = normalizeValue(v)
		}
		result[i] = norm
	}
	return result
}

func normalizeValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case time.Time:
		return val.UTC().Format(time.RFC3339Nano)
	case []byte:
		return string(val)
	case string:
		return strings.TrimSpace(val)
	default:
		return v
	}
}

// buildRecordKey creates a deterministic key for a record
func buildRecordKey(technicalKey string, row map[string]interface{}) string {
	if technicalKey != "" {
		if v, ok := row[technicalKey]; ok && v != nil {
			return fmt.Sprintf("%v", v)
		}
	}
	// Fallback: hash the entire row payload
	b, _ := json.Marshal(row)
	h := sha256.Sum256(b)
	return fmt.Sprintf("%x", h[:8])
}

// UpdateJobStatus updates the execution job record in the internal DB
func (w *Writer) UpdateJobStatus(ctx context.Context, exec *models.ExecutionContext, result *models.ExecutionResult) error {
	_, err := w.db.ExecContext(ctx, `
		INSERT INTO sync_executions
			(execution_id, tenant_id, source_id, dataset_id, connector_type, trigger_type,
			 status, total_records, total_batches, duration_ms, error_message, completed_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (execution_id)
		DO UPDATE SET
			status         = EXCLUDED.status,
			total_records  = EXCLUDED.total_records,
			total_batches  = EXCLUDED.total_batches,
			duration_ms    = EXCLUDED.duration_ms,
			error_message  = EXCLUDED.error_message,
			completed_at   = EXCLUDED.completed_at`,
		exec.ExecutionID, exec.TenantID, exec.SourceID, exec.DatasetID,
		string(exec.ConnectorType), string(exec.TriggerType),
		string(result.Status), result.TotalRecords, result.TotalBatches,
		result.Duration.Milliseconds(), result.Error, result.CompletedAt,
	)
	return err
}
