package main

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/goalfy/goalfy-data-connect-sql/internal/checkpoint"
	"github.com/goalfy/goalfy-data-connect-sql/internal/config"
	"github.com/goalfy/goalfy-data-connect-sql/internal/httpserver"
	"github.com/goalfy/goalfy-data-connect-sql/internal/jobs"
	kafkapkg "github.com/goalfy/goalfy-data-connect-sql/internal/kafka"
	"github.com/goalfy/goalfy-data-connect-sql/internal/locks"
	"github.com/goalfy/goalfy-data-connect-sql/internal/models"
	"github.com/goalfy/goalfy-data-connect-sql/internal/persistence"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// --- Config ---
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// --- Logger ---
	logger, err := buildLogger(cfg.App.LogLevel)
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer logger.Sync()

	logger.Info("starting goalfy-data-connect-sql",
		zap.String("env", cfg.App.Environment),
	)

	// --- PostgreSQL (internal DB) ---
	pgDB, err := sql.Open("postgres", cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("open internal postgres: %w", err)
	}
	pgDB.SetMaxOpenConns(cfg.Postgres.MaxOpenConns)
	pgDB.SetMaxIdleConns(cfg.Postgres.MaxIdleConns)
	pgDB.SetConnMaxLifetime(cfg.Postgres.ConnMaxLifetime)
	defer pgDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := pgDB.PingContext(ctx); err != nil {
		cancel()
		return fmt.Errorf("ping internal postgres: %w", err)
	}
	cancel()

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer rdb.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	if err := rdb.Ping(ctx).Err(); err != nil {
		cancel()
		return fmt.Errorf("ping redis: %w", err)
	}
	cancel()

	// --- Wire dependencies ---
	secretLoader := config.NewSecretLoader(cfg.Secret)
	checkpointMgr := checkpoint.New(pgDB)
	lockMgr := locks.New(rdb, cfg.Redis.LockTTL, cfg.Redis.LockRetries)
	publisher := kafkapkg.NewPublisher(&cfg.Kafka)
	defer publisher.Close()
	writer := persistence.New(pgDB)
	configLoader := jobs.NewDBConfigLoader(pgDB)

	orchestrator := jobs.NewOrchestrator(
		configLoader,
		checkpointMgr,
		lockMgr,
		publisher,
		writer,
		secretLoader,
		logger,
	)

	consumer := kafkapkg.NewConsumer(&cfg.Kafka)
	defer consumer.Close()

	testConsumer := kafkapkg.NewTestConsumer(&cfg.Kafka)
	defer testConsumer.Close()

	discoveryConsumer := kafkapkg.NewDiscoveryConsumer(&cfg.Kafka)
	defer discoveryConsumer.Close()

	checkpointResetConsumer := kafkapkg.NewCheckpointResetConsumer(&cfg.Kafka)
	defer checkpointResetConsumer.Close()

	// --- HTTP server ---
	httpSrv := httpserver.New(orchestrator, logger)

	server := &http.Server{
		Addr:    ":8081",
		Handler: httpSrv.Handler(),
	}

	go func() {
		logger.Info("http server listening", zap.String("addr", ":8081"))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("http server failed", zap.Error(err))
		}
	}()

	// --- Graceful shutdown ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	go func() {
		sig := <-sigCh
		logger.Info("received shutdown signal", zap.String("signal", sig.String()))
		workerCancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("http server shutdown failed", zap.Error(err))
		}
	}()

	// --- Test connection event loop ---
	go func() {
		retryCount := 0
		for {
			event, err := testConsumer.ReadTestConnectionEvent(workerCtx)
			if err != nil {
				if workerCtx.Err() != nil {
					return
				}
				retryCount++
				logger.Error("failed to read test connection event", zap.Error(err), zap.Int("retry", retryCount))
				time.Sleep(2 * time.Second)
				continue
			}
			retryCount = 0
			go func(ev *models.TestConnectionEvent) {
				result, err := orchestrator.TestConnectionByID(workerCtx, ev.TenantID, ev.SourceID, ev.RequestID)
				if err != nil {
					logger.Error("test connection failed",
						zap.String("request_id", ev.RequestID),
						zap.String("tenant_id", ev.TenantID),
						zap.String("source_id", ev.SourceID),
						zap.Error(err),
					)
					return
				}
				if err := publisher.PublishTestConnectionResult(workerCtx, result); err != nil {
					logger.Warn("failed to publish test connection result", zap.Error(err))
				}
			}(event)
		}
	}()

	// --- Schema discovery event loop ---
	go func() {
		retryCount := 0
		for {
			event, err := discoveryConsumer.ReadDiscoveryEvent(workerCtx)
			if err != nil {
				if workerCtx.Err() != nil {
					return
				}
				retryCount++
				logger.Error("failed to read discovery event", zap.Error(err), zap.Int("retry", retryCount))
				time.Sleep(2 * time.Second)
				continue
			}
			retryCount = 0
			go func(ev *models.DiscoveryEvent) {
				schema, err := orchestrator.DiscoverSchemaByID(workerCtx, ev.TenantID, ev.SourceID, ev.FilterSchema, ev.AllowedTables)
				if err != nil {
					logger.Error("schema discovery failed",
						zap.String("request_id", ev.RequestID),
						zap.String("tenant_id", ev.TenantID),
						zap.String("source_id", ev.SourceID),
						zap.Error(err),
					)
					_ = publisher.PublishDiscoveryResult(workerCtx, &models.DiscoveryResult{
						RequestID: ev.RequestID,
						TenantID:  ev.TenantID,
						SourceID:  ev.SourceID,
						Success:   false,
						Error:     err.Error(),
						Timestamp: time.Now().UTC(),
					})
					return
				}
				_ = publisher.PublishDiscoveryResult(workerCtx, &models.DiscoveryResult{
					RequestID:   ev.RequestID,
					TenantID:    ev.TenantID,
					SourceID:    ev.SourceID,
					Success:     true,
					SchemaCount: len(schema.Schemas),
					TableCount:  len(schema.Tables),
					Timestamp:   time.Now().UTC(),
				})
			}(event)
		}
	}()

	// --- Checkpoint reset event loop ---
	go func() {
		retryCount := 0
		for {
			event, err := checkpointResetConsumer.ReadCheckpointResetEvent(workerCtx)
			if err != nil {
				if workerCtx.Err() != nil {
					return
				}
				retryCount++
				logger.Error("failed to read checkpoint reset event", zap.Error(err), zap.Int("retry", retryCount))
				time.Sleep(2 * time.Second)
				continue
			}
			retryCount = 0
			if err := orchestrator.ResetCheckpoint(workerCtx, event.TenantID, event.SourceID, event.DatasetID); err != nil {
				logger.Error("checkpoint reset failed",
					zap.String("request_id", event.RequestID),
					zap.String("tenant_id", event.TenantID),
					zap.String("source_id", event.SourceID),
					zap.String("dataset_id", event.DatasetID),
					zap.Error(err),
				)
			} else {
				logger.Info("checkpoint reset successfully",
					zap.String("request_id", event.RequestID),
					zap.String("tenant_id", event.TenantID),
					zap.String("source_id", event.SourceID),
					zap.String("dataset_id", event.DatasetID),
				)
			}
		}
	}()

	// --- Event loop ---
	logger.Info("listening for sync events", zap.String("topic", cfg.Kafka.TopicSyncReq))

	retryCount := 0
	for {
		event, err := consumer.ReadSyncEvent(workerCtx)
		if err != nil {
			if workerCtx.Err() != nil {
				logger.Info("worker context cancelled, shutting down")
				return nil
			}
			retryCount++
			logger.Error("failed to read sync event", zap.Error(err), zap.Int("retry", retryCount))
			time.Sleep(2 * time.Second)
			continue
		}
		retryCount = 0

		logger.Info("received sync event",
			zap.String("execution_id", event.ExecutionID),
			zap.String("tenant_id", event.TenantID),
			zap.String("dataset_id", event.DatasetID),
		)

		go func(ev *models.SyncEvent) {
			execCtx, execCancel := context.WithTimeout(workerCtx, cfg.App.ShutdownTimeout*10)
			defer execCancel()

			if err := orchestrator.Execute(execCtx, ev); err != nil {
				logger.Error("sync execution error",
					zap.String("execution_id", ev.ExecutionID),
					zap.Error(err),
				)
			}
		}(event)
	}
}

func buildLogger(level string) (*zap.Logger, error) {
	lvl := zapcore.InfoLevel
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = zapcore.InfoLevel
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	return cfg.Build()
}
