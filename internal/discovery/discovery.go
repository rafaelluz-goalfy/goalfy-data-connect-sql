package discovery

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// Discoverer handles schema introspection for external databases
type Discoverer struct {
	db            *sql.DB
	connectorType models.ConnectorType
}

// New creates a new Discoverer
func New(db *sql.DB, connectorType models.ConnectorType) *Discoverer {
	return &Discoverer{db: db, connectorType: connectorType}
}

// Discover runs full schema discovery and returns the result
func (d *Discoverer) Discover(ctx context.Context, tenantID, sourceID string, filterSchema string, allowedTables []string) (*models.SchemaDiscovery, error) {
	schemas, err := d.ListSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing schemas: %w", err)
	}

	var tables []models.TableInfo
	for _, schema := range schemas {
		if filterSchema != "" && schema != filterSchema {
			continue
		}
		t, err := d.ListTablesAndViews(ctx, schema, allowedTables)
		if err != nil {
			return nil, fmt.Errorf("listing tables for schema %s: %w", schema, err)
		}
		tables = append(tables, t...)
	}

	return &models.SchemaDiscovery{
		TenantID:     tenantID,
		SourceID:     sourceID,
		Schemas:      schemas,
		Tables:       tables,
		DiscoveredAt: time.Now().UTC(),
	}, nil
}

// ListSchemas returns all user-accessible schemas
func (d *Discoverer) ListSchemas(ctx context.Context) ([]string, error) {
	var query string
	switch d.connectorType {
	case models.ConnectorPostgres:
		query = `SELECT schema_name FROM information_schema.schemata
				 WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast')
				 ORDER BY schema_name`
	case models.ConnectorMySQL:
		query = `SELECT schema_name FROM information_schema.schemata
				 WHERE schema_name NOT IN ('mysql','information_schema','performance_schema','sys')
				 ORDER BY schema_name`
	case models.ConnectorSQLServer:
		query = `SELECT name FROM sys.schemas
				 WHERE principal_id = 1
				 ORDER BY name`
	default:
		return nil, fmt.Errorf("unsupported connector: %s", d.connectorType)
	}

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, rows.Err()
}

// ListTablesAndViews returns all tables and views for a given schema with column info.
// If allowedTables is non-empty, only tables in that list are returned.
func (d *Discoverer) ListTablesAndViews(ctx context.Context, schemaName string, allowedTables []string) ([]models.TableInfo, error) {
	objects, err := d.listObjects(ctx, schemaName)
	if err != nil {
		return nil, err
	}

	if len(allowedTables) > 0 {
		allowed := make(map[string]struct{}, len(allowedTables))
		for _, t := range allowedTables {
			allowed[t] = struct{}{}
		}
		filtered := objects[:0]
		for _, obj := range objects {
			if _, ok := allowed[obj.TableName]; ok {
				filtered = append(filtered, obj)
			}
		}
		objects = filtered
	}

	for i := range objects {
		cols, err := d.listColumns(ctx, schemaName, objects[i].TableName)
		if err != nil {
			return nil, fmt.Errorf("listing columns for %s.%s: %w", schemaName, objects[i].TableName, err)
		}
		objects[i].Columns = cols
	}

	return objects, nil
}

func (d *Discoverer) listObjects(ctx context.Context, schemaName string) ([]models.TableInfo, error) {
	var query string
	switch d.connectorType {
	case models.ConnectorPostgres:
		query = `SELECT table_name, table_type FROM information_schema.tables
				 WHERE table_schema = $1 ORDER BY table_name`
	case models.ConnectorMySQL:
		query = `SELECT table_name, table_type FROM information_schema.tables
				 WHERE table_schema = ? ORDER BY table_name`
	case models.ConnectorSQLServer:
		query = `SELECT t.name, t.type_desc FROM sys.objects t
				 INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
				 WHERE s.name = @p1 AND t.type IN ('U','V') ORDER BY t.name`
	default:
		return nil, fmt.Errorf("unsupported connector: %s", d.connectorType)
	}

	rows, err := d.db.QueryContext(ctx, query, schemaName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []models.TableInfo
	for rows.Next() {
		var name, objType string
		if err := rows.Scan(&name, &objType); err != nil {
			return nil, err
		}
		normalized := "table"
		if objType == "VIEW" || objType == "VIEW (SQL View)" || objType == "V" {
			normalized = "view"
		}
		objects = append(objects, models.TableInfo{
			SchemaName: schemaName,
			TableName:  name,
			ObjectType: normalized,
		})
	}
	return objects, rows.Err()
}

func (d *Discoverer) listColumns(ctx context.Context, schemaName, tableName string) ([]models.ColumnInfo, error) {
	var query string
	switch d.connectorType {
	case models.ConnectorPostgres:
		query = `
			SELECT
				c.column_name,
				c.data_type,
				c.is_nullable = 'YES' AS nullable,
				COALESCE(tc.constraint_type = 'PRIMARY KEY', false) AS is_primary,
				c.ordinal_position
			FROM information_schema.columns c
			LEFT JOIN information_schema.key_column_usage kcu
				ON kcu.table_schema = c.table_schema
				AND kcu.table_name = c.table_name
				AND kcu.column_name = c.column_name
			LEFT JOIN information_schema.table_constraints tc
				ON tc.constraint_name = kcu.constraint_name
				AND tc.constraint_type = 'PRIMARY KEY'
			WHERE c.table_schema = $1 AND c.table_name = $2
			ORDER BY c.ordinal_position`
	case models.ConnectorMySQL:
		query = `
			SELECT
				c.column_name,
				c.data_type,
				c.is_nullable = 'YES' AS nullable,
				c.column_key = 'PRI' AS is_primary,
				c.ordinal_position
			FROM information_schema.columns c
			WHERE c.table_schema = ? AND c.table_name = ?
			ORDER BY c.ordinal_position`
	case models.ConnectorSQLServer:
		query = `
			SELECT
				c.name,
				tp.name AS data_type,
				c.is_nullable,
				CAST(COALESCE(ic.index_column_id, 0) AS BIT) AS is_primary,
				c.column_id
			FROM sys.columns c
			INNER JOIN sys.objects obj ON c.object_id = obj.object_id
			INNER JOIN sys.schemas s ON obj.schema_id = s.schema_id
			INNER JOIN sys.types tp ON c.user_type_id = tp.user_type_id
			LEFT JOIN sys.index_columns ic
				ON ic.object_id = c.object_id
				AND ic.column_id = c.column_id
				AND ic.index_id = 1
			WHERE s.name = @p1 AND obj.name = @p2
			ORDER BY c.column_id`
	default:
		return nil, fmt.Errorf("unsupported connector: %s", d.connectorType)
	}

	rows, err := d.db.QueryContext(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []models.ColumnInfo
	for rows.Next() {
		var col models.ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType, &col.Nullable, &col.IsPrimary, &col.OrdinalPos); err != nil {
			return nil, err
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}
