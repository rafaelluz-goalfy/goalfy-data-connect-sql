package checkpoint

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// Manager handles checkpoint persistence for incremental syncs
type Manager struct {
	db *sql.DB
}

// New creates a new Manager
func New(db *sql.DB) *Manager {
	return &Manager{db: db}
}

// Load retrieves the last checkpoint for a dataset, returns nil if none exists
func (m *Manager) Load(ctx context.Context, tenantID, sourceID, datasetID string) (*models.Checkpoint, error) {
	const q = `
		SELECT tenant_id, source_id, dataset_id, last_value, updated_at
		FROM sync_checkpoints
		WHERE tenant_id = $1 AND source_id = $2 AND dataset_id = $3
		LIMIT 1`

	row := m.db.QueryRowContext(ctx, q, tenantID, sourceID, datasetID)
	cp := &models.Checkpoint{}
	err := row.Scan(&cp.TenantID, &cp.SourceID, &cp.DatasetID, &cp.LastValue, &cp.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loading checkpoint: %w", err)
	}
	return cp, nil
}

// Save upserts the checkpoint for a dataset
func (m *Manager) Save(ctx context.Context, cp *models.Checkpoint) error {
	cp.UpdatedAt = time.Now().UTC()
	const q = `
		INSERT INTO sync_checkpoints (tenant_id, source_id, dataset_id, last_value, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, source_id, dataset_id)
		DO UPDATE SET last_value = EXCLUDED.last_value, updated_at = EXCLUDED.updated_at`

	_, err := m.db.ExecContext(ctx, q, cp.TenantID, cp.SourceID, cp.DatasetID, cp.LastValue, cp.UpdatedAt)
	if err != nil {
		return fmt.Errorf("saving checkpoint: %w", err)
	}
	return nil
}

// Delete removes the checkpoint for a dataset (used on full reload)
func (m *Manager) Delete(ctx context.Context, tenantID, sourceID, datasetID string) error {
	const q = `DELETE FROM sync_checkpoints WHERE tenant_id = $1 AND source_id = $2 AND dataset_id = $3`
	_, err := m.db.ExecContext(ctx, q, tenantID, sourceID, datasetID)
	return err
}
