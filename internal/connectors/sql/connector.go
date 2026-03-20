package sql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
)

// Connector abstracts a connection to an external SQL database
type Connector interface {
	Connect(ctx context.Context) error
	Ping(ctx context.Context) error
	Close() error
	DB() *sql.DB
	TestConnection(ctx context.Context) (*ConnectionTestResult, error)
}

// ConnectionTestResult holds the output of a connection test
type ConnectionTestResult struct {
	Success      bool
	Message      string
	ResponseTime time.Duration
	Error        string
}

// baseConnector is the shared implementation
type baseConnector struct {
	db     *sql.DB
	dsn    string
	driver string
	schema string
}

func (c *baseConnector) Connect(ctx context.Context) error {
	db, err := sql.Open(c.driver, c.dsn)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	c.db = db
	return nil
}

func (c *baseConnector) Ping(ctx context.Context) error {
	if c.db == nil {
		return fmt.Errorf("connection not established")
	}
	return c.db.PingContext(ctx)
}

func (c *baseConnector) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *baseConnector) DB() *sql.DB {
	return c.db
}

func (c *baseConnector) TestConnection(ctx context.Context) (*ConnectionTestResult, error) {
	start := time.Now()

	// Step 1: open connection — validates host, authentication and database
	if err := c.Connect(ctx); err != nil {
		return &ConnectionTestResult{
			Success:      false,
			Message:      "host unreachable, authentication failed, or database not found",
			ResponseTime: time.Since(start),
			Error:        err.Error(),
		}, nil
	}
	defer c.Close()

	// Step 2: ping — confirms the connection is alive
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.Ping(pingCtx); err != nil {
		return &ConnectionTestResult{
			Success:      false,
			Message:      "host reachable but database ping failed",
			ResponseTime: time.Since(start),
			Error:        err.Error(),
		}, nil
	}

	// Step 3: validate schema access (if a schema is configured)
	if c.schema != "" {
		if err := c.validateSchemaAccess(ctx); err != nil {
			return &ConnectionTestResult{
				Success:      false,
				Message:      fmt.Sprintf("connection ok but schema %q is not accessible", c.schema),
				ResponseTime: time.Since(start),
				Error:        err.Error(),
			}, nil
		}
	}

	return &ConnectionTestResult{
		Success:      true,
		Message:      "connection validated: host reachable, authentication ok, database and schema accessible",
		ResponseTime: time.Since(start),
	}, nil
}

func (c *baseConnector) validateSchemaAccess(ctx context.Context) error {
	var query string
	switch c.driver {
	case "postgres":
		query = `SELECT 1 FROM information_schema.schemata WHERE schema_name = $1`
	case "mysql":
		query = `SELECT 1 FROM information_schema.schemata WHERE schema_name = ?`
	case "sqlserver":
		query = `SELECT 1 FROM sys.schemas WHERE name = @p1`
	default:
		return fmt.Errorf("unsupported driver for schema validation: %s", c.driver)
	}

	row := c.db.QueryRowContext(ctx, query, c.schema)
	var dummy int
	if err := row.Scan(&dummy); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("schema %q does not exist or is not accessible to the configured user", c.schema)
		}
		return fmt.Errorf("schema validation query failed: %w", err)
	}
	return nil
}

// NewConnector creates a Connector for the given source config and resolved password
func NewConnector(source *models.SourceConfig, password string) (Connector, error) {
	switch source.ConnectorType {
	case models.ConnectorPostgres:
		dsn := buildPostgresDSN(source, password)
		return &baseConnector{dsn: dsn, driver: "postgres", schema: source.Schema}, nil

	case models.ConnectorMySQL:
		dsn := buildMySQLDSN(source, password)
		return &baseConnector{dsn: dsn, driver: "mysql", schema: source.Schema}, nil

	case models.ConnectorSQLServer:
		dsn := buildSQLServerDSN(source, password)
		return &baseConnector{dsn: dsn, driver: "sqlserver", schema: source.Schema}, nil

	default:
		return nil, fmt.Errorf("unsupported connector type: %s", source.ConnectorType)
	}
}

func buildPostgresDSN(s *models.SourceConfig, password string) string {
	sslMode := s.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s&connect_timeout=10",
		url.QueryEscape(s.Username),
		url.QueryEscape(password),
		s.Host,
		s.Port,
		s.Database,
		sslMode,
	)
}

func buildMySQLDSN(s *models.SourceConfig, password string) string {
	tls := "false"
	if s.SSLMode == "require" {
		tls = "true"
	}
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&tls=%s&timeout=10s",
		s.Username, password, s.Host, s.Port, s.Database, tls,
	)
}

func buildSQLServerDSN(s *models.SourceConfig, password string) string {
	return fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s&connection+timeout=10",
		s.Username, password, s.Host, s.Port, s.Database,
	)
}
