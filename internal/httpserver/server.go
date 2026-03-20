package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/goalfy/goalfy-data-connect-sql/internal/jobs"
	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
	"go.uber.org/zap"
)

type Server struct {
	orchestrator *jobs.Orchestrator
	logger       *zap.Logger
	mux          *http.ServeMux
}

type TestConnectionRequest struct {
	TenantID      string `json:"tenant_id"`
	SourceID      string `json:"source_id"` // optional — used for tracing/logging only
	RequestID     string `json:"request_id"`
	ConnectorType string `json:"connector_type"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Database      string `json:"database"`
	Schema        string `json:"schema"`
	Username      string `json:"username"`
	PasswordRef   string `json:"password_ref"`
	SSLMode       string `json:"ssl_mode"`
}

type DiscoveryRequest struct {
	TenantID      string   `json:"tenant_id"`
	SourceID      string   `json:"source_id"` // optional — tracing/logging only
	RequestID     string   `json:"request_id"`
	ConnectorType string   `json:"connector_type"`
	Host          string   `json:"host"`
	Port          int      `json:"port"`
	Database      string   `json:"database"`
	Schema        string   `json:"schema"`
	Username      string   `json:"username"`
	PasswordRef   string   `json:"password_ref"`
	SSLMode       string   `json:"ssl_mode"`
	FilterSchema  string   `json:"filter_schema"`
	AllowedTables []string `json:"allowed_tables"`
}

type ResetCheckpointRequest struct {
	TenantID  string `json:"tenant_id"`
	SourceID  string `json:"source_id"`
	DatasetID string `json:"dataset_id"`
}

type UpsertSourceRequest struct {
	TenantID      string `json:"tenant_id"`
	SourceID      string `json:"source_id"`
	ConnectorType string `json:"connector_type"`
	Host          string `json:"host"`
	Port          int    `json:"port"`
	Database      string `json:"database"`
	Schema        string `json:"schema"`
	Username      string `json:"username"`
	PasswordRef   string `json:"password_ref"`
	SSLMode       string `json:"ssl_mode"`
}

type UpsertDatasetRequest struct {
	TenantID          string `json:"tenant_id"`
	DatasetID         string `json:"dataset_id"`
	TableName         string `json:"table_name"`
	SyncMode          string `json:"sync_mode"`
	SchemaName        string `json:"schema_name"`
	ObjectType        string `json:"object_type"`
	IncrementalColumn string `json:"incremental_column"`
	IncrementalType   string `json:"incremental_type"`
	TechnicalKey      string `json:"technical_key"`
	BatchSize         int    `json:"batch_size"`
	Enabled           *bool  `json:"enabled"`
}

func New(orchestrator *jobs.Orchestrator, logger *zap.Logger) *Server {
	s := &Server{
		orchestrator: orchestrator,
		logger:       logger,
		mux:          http.NewServeMux(),
	}

	s.routes()
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", s.handleHealth)
	s.mux.HandleFunc("/sync", s.handleSync)
	s.mux.HandleFunc("/test-connection", s.handleTestConnection)
	s.mux.HandleFunc("/discovery", s.handleDiscovery)
	s.mux.HandleFunc("/checkpoint/reset", s.handleResetCheckpoint)
	s.mux.HandleFunc("/sources", s.handleUpsertSource)
	s.mux.HandleFunc("/sources/{source_id}/datasets", s.handleUpsertDataset)
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"status": "ok",
	})
}

func (s *Server) handleSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.SyncEvent
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	s.logger.Info("received sync request via http",
		zap.String("execution_id", req.ExecutionID),
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", req.SourceID),
		zap.String("dataset_id", req.DatasetID),
	)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		s.logger.Info("starting sync execution via http",
			zap.String("execution_id", req.ExecutionID),
		)

		if err := s.orchestrator.Execute(ctx, &req); err != nil {
			s.logger.Error("sync execution error via http",
				zap.String("execution_id", req.ExecutionID),
				zap.Error(err),
			)
		}
	}()

	writeJSON(w, http.StatusAccepted, map[string]string{
		"message":      "sync started",
		"execution_id": req.ExecutionID,
	})
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req TestConnectionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.RequestID == "" || req.ConnectorType == "" ||
		req.Host == "" || req.Port == 0 || req.Database == "" ||
		req.Username == "" || req.PasswordRef == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "tenant_id, request_id, connector_type, host, port, database, username and password_ref are required",
		})
		return
	}

	if req.SSLMode == "" {
		req.SSLMode = "disable"
	}

	s.logger.Info("received test connection request via http",
		zap.String("request_id", req.RequestID),
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", req.SourceID),
		zap.String("connector_type", req.ConnectorType),
		zap.String("host", req.Host),
	)

	source := &models.SourceConfig{
		TenantID:      req.TenantID,
		SourceID:      req.SourceID,
		ConnectorType: models.ConnectorType(req.ConnectorType),
		Host:          req.Host,
		Port:          req.Port,
		Database:      req.Database,
		Schema:        req.Schema,
		Username:      req.Username,
		PasswordRef:   req.PasswordRef,
		SSLMode:       req.SSLMode,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	result, err := s.orchestrator.TestConnection(ctx, source, req.RequestID)
	if err != nil {
		s.logger.Error("test connection error via http",
			zap.String("request_id", req.RequestID),
			zap.String("tenant_id", req.TenantID),
			zap.String("source_id", req.SourceID),
			zap.Error(err),
		)

		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req DiscoveryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.RequestID == "" || req.ConnectorType == "" ||
		req.Host == "" || req.Port == 0 || req.Database == "" ||
		req.Username == "" || req.PasswordRef == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "tenant_id, request_id, connector_type, host, port, database, username and password_ref are required",
		})
		return
	}

	if req.SSLMode == "" {
		req.SSLMode = "disable"
	}

	s.logger.Info("received discovery request via http",
		zap.String("request_id", req.RequestID),
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", req.SourceID),
		zap.String("connector_type", req.ConnectorType),
		zap.String("host", req.Host),
		zap.String("filter_schema", req.FilterSchema),
	)

	source := &models.SourceConfig{
		TenantID:      req.TenantID,
		SourceID:      req.SourceID,
		ConnectorType: models.ConnectorType(req.ConnectorType),
		Host:          req.Host,
		Port:          req.Port,
		Database:      req.Database,
		Schema:        req.Schema,
		Username:      req.Username,
		PasswordRef:   req.PasswordRef,
		SSLMode:       req.SSLMode,
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	result, err := s.orchestrator.DiscoverSchema(
		ctx,
		source,
		req.FilterSchema,
		req.AllowedTables,
	)
	if err != nil {
		s.logger.Error("discovery error via http",
			zap.String("request_id", req.RequestID),
			zap.String("tenant_id", req.TenantID),
			zap.String("source_id", req.SourceID),
			zap.Error(err),
		)

		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) handleResetCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ResetCheckpointRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.SourceID == "" || req.DatasetID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "tenant_id, source_id and dataset_id are required",
		})
		return
	}

	s.logger.Info("received checkpoint reset request via http",
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", req.SourceID),
		zap.String("dataset_id", req.DatasetID),
	)

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	if err := s.orchestrator.ResetCheckpoint(ctx, req.TenantID, req.SourceID, req.DatasetID); err != nil {
		s.logger.Error("checkpoint reset error via http",
			zap.String("tenant_id", req.TenantID),
			zap.String("source_id", req.SourceID),
			zap.String("dataset_id", req.DatasetID),
			zap.Error(err),
		)

		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"message":    "checkpoint reset successfully",
		"tenant_id":  req.TenantID,
		"source_id":  req.SourceID,
		"dataset_id": req.DatasetID,
	})
}

func (s *Server) handleUpsertSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req UpsertSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.SourceID == "" || req.ConnectorType == "" ||
		req.Host == "" || req.Port == 0 || req.Database == "" ||
		req.Username == "" || req.PasswordRef == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "tenant_id, source_id, connector_type, host, port, database, username and password_ref are required",
		})
		return
	}

	if req.SSLMode == "" {
		req.SSLMode = "disable"
	}

	source := &models.SourceConfig{
		TenantID:      req.TenantID,
		SourceID:      req.SourceID,
		ConnectorType: models.ConnectorType(req.ConnectorType),
		Host:          req.Host,
		Port:          req.Port,
		Database:      req.Database,
		Schema:        req.Schema,
		Username:      req.Username,
		PasswordRef:   req.PasswordRef,
		SSLMode:       req.SSLMode,
	}

	s.logger.Info("upserting source config",
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", req.SourceID),
		zap.String("connector_type", req.ConnectorType),
	)

	if err := s.orchestrator.UpsertSource(r.Context(), source); err != nil {
		s.logger.Error("upsert source error",
			zap.String("tenant_id", req.TenantID),
			zap.String("source_id", req.SourceID),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"tenant_id": req.TenantID,
		"source_id": req.SourceID,
	})
}

func (s *Server) handleUpsertDataset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sourceID := r.PathValue("source_id")
	if sourceID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "source_id is required in path",
		})
		return
	}

	var req UpsertDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if req.TenantID == "" || req.DatasetID == "" || req.TableName == "" || req.SyncMode == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "tenant_id, dataset_id, table_name and sync_mode are required",
		})
		return
	}

	if req.ObjectType == "" {
		req.ObjectType = "table"
	}
	if req.BatchSize == 0 {
		req.BatchSize = 1000
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	dataset := &models.DatasetConfig{
		TenantID:          req.TenantID,
		SourceID:          sourceID,
		DatasetID:         req.DatasetID,
		SchemaName:        req.SchemaName,
		TableName:         req.TableName,
		ObjectType:        req.ObjectType,
		SyncMode:          models.SyncMode(req.SyncMode),
		IncrementalColumn: req.IncrementalColumn,
		IncrementalType:   models.IncrementalType(req.IncrementalType),
		TechnicalKey:      req.TechnicalKey,
		BatchSize:         req.BatchSize,
		Enabled:           enabled,
	}

	s.logger.Info("upserting dataset config",
		zap.String("tenant_id", req.TenantID),
		zap.String("source_id", sourceID),
		zap.String("dataset_id", req.DatasetID),
	)

	if err := s.orchestrator.UpsertDataset(r.Context(), dataset); err != nil {
		s.logger.Error("upsert dataset error",
			zap.String("tenant_id", req.TenantID),
			zap.String("source_id", sourceID),
			zap.String("dataset_id", req.DatasetID),
			zap.Error(err),
		)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"tenant_id":  req.TenantID,
		"source_id":  sourceID,
		"dataset_id": req.DatasetID,
	})
}

func writeJSON(w http.ResponseWriter, status int, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
