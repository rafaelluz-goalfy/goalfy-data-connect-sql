package reader

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// BatchHandler is called for each batch of records read
type BatchHandler func(ctx context.Context, batch []map[string]interface{}, batchNum int) error

// Reader executes dataset reads against an external SQL source
type Reader struct {
	db            *sql.DB
	connectorType models.ConnectorType
	logger        *zap.Logger
}

// New creates a new Reader
func New(db *sql.DB, connectorType models.ConnectorType, logger *zap.Logger) *Reader {
	return &Reader{db: db, connectorType: connectorType, logger: logger}
}

// ReadFull reads all rows in the configured table, calling handler per batch
func (r *Reader) ReadFull(ctx context.Context, dataset *models.DatasetConfig, handler BatchHandler) (int, error) {
	table := r.qualifiedTable(dataset)
	batchSize := dataset.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Use keyset pagination when technical key is set to avoid OFFSET on large tables
	if dataset.TechnicalKey != "" {
		n, _, err := r.readByKey(ctx, table, dataset.TechnicalKey, batchSize, "", handler)
		return n, err
	}
	return r.readByOffset(ctx, table, batchSize, handler)
}

// ReadIncremental reads only rows newer than the checkpoint value
func (r *Reader) ReadIncremental(ctx context.Context, dataset *models.DatasetConfig, lastValue string, handler BatchHandler) (int, string, error) {
	table := r.qualifiedTable(dataset)
	batchSize := dataset.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	col := dataset.IncrementalColumn
	if col == "" {
		return 0, "", fmt.Errorf("incremental_column not configured for dataset %s", dataset.DatasetID)
	}

	var newLastValue string
	var totalRead int

	switch dataset.IncrementalType {
	case models.IncrementalByDate:
		n, last, err := r.readIncrementalByDate(ctx, table, col, lastValue, batchSize, handler)
		if err != nil {
			return 0, "", err
		}
		totalRead = n
		newLastValue = last

	case models.IncrementalByKey:
		n, last, err := r.readIncrementalByKey(ctx, table, col, lastValue, batchSize, handler)
		if err != nil {
			return 0, "", err
		}
		totalRead = n
		newLastValue = last

	default:
		return 0, "", fmt.Errorf("unsupported incremental type: %s", dataset.IncrementalType)
	}

	return totalRead, newLastValue, nil
}

// --- internal strategies ---

func (r *Reader) readByOffset(ctx context.Context, table string, batchSize int, handler BatchHandler) (int, error) {
	offset := 0
	batchNum := 1
	total := 0

	for {
		query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, batchSize, offset)
		rows, err := r.db.QueryContext(ctx, query)
		if err != nil {
			return total, fmt.Errorf("query offset batch: %w", err)
		}

		batch, err := scanRows(rows)
		rows.Close()
		if err != nil {
			return total, err
		}
		if len(batch) == 0 {
			break
		}

		if err := handler(ctx, batch, batchNum); err != nil {
			return total, fmt.Errorf("batch handler error at batch %d: %w", batchNum, err)
		}

		total += len(batch)
		offset += len(batch)
		batchNum++

		if len(batch) < batchSize {
			break
		}
	}
	return total, nil
}

func (r *Reader) readByKey(ctx context.Context, table, keyCol string, batchSize int, startAfter string, handler BatchHandler) (int, string, error) {
	lastKey := startAfter
	batchNum := 1
	total := 0

	for {
		var query string
		var args []interface{}

		if lastKey == "" {
			query = fmt.Sprintf("SELECT * FROM %s ORDER BY %s LIMIT %d", table, keyCol, batchSize)
		} else {
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d", table, keyCol, keyCol, batchSize)
			args = append(args, lastKey)
		}

		rows, err := r.db.QueryContext(ctx, query, args...)
		if err != nil {
			return total, lastKey, fmt.Errorf("query keyset batch: %w", err)
		}

		batch, err := scanRows(rows)
		rows.Close()
		if err != nil {
			return total, lastKey, err
		}
		if len(batch) == 0 {
			break
		}

		if err := handler(ctx, batch, batchNum); err != nil {
			return total, lastKey, fmt.Errorf("batch handler error at batch %d: %w", batchNum, err)
		}

		total += len(batch)
		batchNum++

		// track last key for next iteration
		lastRow := batch[len(batch)-1]
		if v, ok := lastRow[keyCol]; ok {
			lastKey = fmt.Sprintf("%v", v)
		}

		if len(batch) < batchSize {
			break
		}
	}
	return total, lastKey, nil
}

func (r *Reader) readIncrementalByDate(ctx context.Context, table, col, lastValue string, batchSize int, handler BatchHandler) (int, string, error) {
	var whereClause string
	var args []interface{}

	if lastValue != "" {
		whereClause = fmt.Sprintf("WHERE %s > $1", col)
		args = append(args, lastValue)
	}

	query := fmt.Sprintf("SELECT * FROM %s %s ORDER BY %s LIMIT %d", table, whereClause, col, batchSize)

	batchNum := 1
	total := 0
	newLastValue := lastValue

	for {
		var batchArgs []interface{}
		batchArgs = append(batchArgs, args...)

		rows, err := r.db.QueryContext(ctx, query, batchArgs...)
		if err != nil {
			return total, newLastValue, fmt.Errorf("incremental date query: %w", err)
		}

		batch, err := scanRows(rows)
		rows.Close()
		if err != nil {
			return total, newLastValue, err
		}
		if len(batch) == 0 {
			break
		}

		if err := handler(ctx, batch, batchNum); err != nil {
			return total, newLastValue, err
		}

		total += len(batch)
		batchNum++

		lastRow := batch[len(batch)-1]
		if v, ok := lastRow[col]; ok {
			if t, ok := v.(time.Time); ok {
				newLastValue = t.UTC().Format(time.RFC3339Nano)
			} else {
				newLastValue = fmt.Sprintf("%v", v)
			}
		}

		if len(batch) < batchSize {
			break
		}
		// For simplicity use offset here; in production use cursor on date+key combo
		if lastValue == "" {
			query = fmt.Sprintf("SELECT * FROM %s WHERE %s > $1 ORDER BY %s LIMIT %d", table, col, col, batchSize)
		}
		args = []interface{}{newLastValue}
	}

	return total, newLastValue, nil
}

func (r *Reader) readIncrementalByKey(ctx context.Context, table, col, lastValue string, batchSize int, handler BatchHandler) (int, string, error) {
	total, lastKey, err := r.readByKey(ctx, table, col, batchSize, lastValue, handler)
	if err != nil {
		return 0, lastValue, err
	}
	return total, lastKey, nil
}

// --- helpers ---

func (r *Reader) qualifiedTable(dataset *models.DatasetConfig) string {
	if dataset.SchemaName != "" {
		return fmt.Sprintf("%s.%s", quoteIdentifier(dataset.SchemaName), quoteIdentifier(dataset.TableName))
	}
	return quoteIdentifier(dataset.TableName)
}

func quoteIdentifier(s string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(s, `"`, `""`))
}

func scanRows(rows *sql.Rows) ([]map[string]interface{}, error) {
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			row[col] = values[i]
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
