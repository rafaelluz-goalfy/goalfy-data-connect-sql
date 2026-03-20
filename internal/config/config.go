package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	App      AppConfig
	Kafka    KafkaConfig
	Redis    RedisConfig
	Postgres PostgresConfig
	Secret   SecretConfig
}

type HTTPConfig struct {
    Port string
}

type AppConfig struct {
	Name            string
	Environment     string
	LogLevel        string
	ShutdownTimeout time.Duration
}

type KafkaConfig struct {
	Brokers              []string
	GroupID              string
	TopicSyncReq         string
	TopicStarted         string
	TopicBatch           string
	TopicCompleted       string
	TopicFailed          string
	TopicTestReq         string
	TopicDiscoveryReq    string
	TopicCheckpointReset string
	TopicTestResult      string
	TopicDiscoveryResult string
	MaxBytes             int
	CommitInterval       time.Duration
}

type RedisConfig struct {
	Addr        string
	Password    string
	DB          int
	LockTTL     time.Duration
	LockRetries int
}

type PostgresConfig struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

type SecretConfig struct {
	// In production, integrate with GCP Secret Manager or Vault
	// For now, supports env-based secrets via prefix
	Provider string // "env" | "gcp" | "vault"
	Prefix   string
}

// Load reads configuration from environment variables and config files
func Load() (*Config, error) {
	v := viper.New()
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("app.name", "goalfy-data-connect-sql")
	v.SetDefault("app.environment", "development")
	v.SetDefault("app.log_level", "info")
	v.SetDefault("app.shutdown_timeout", "30s")

	v.SetDefault("kafka.group_id", "goalfy-data-connect-sql")
	v.SetDefault("kafka.topic_sync_req", "data.connect.sync.requested")
	v.SetDefault("kafka.topic_started", "data.connect.sync.started")
	v.SetDefault("kafka.topic_batch", "data.connect.batch.processed")
	v.SetDefault("kafka.topic_completed", "data.connect.sync.completed")
	v.SetDefault("kafka.topic_failed", "data.connect.sync.failed")
	v.SetDefault("kafka.topic_test_req", "data.connect.test.requested")
	v.SetDefault("kafka.topic_discovery_req", "data.connect.discovery.requested")
	v.SetDefault("kafka.topic_checkpoint_reset", "data.connect.checkpoint.reset")
	v.SetDefault("kafka.topic_test_result", "data.connect.test.result")
	v.SetDefault("kafka.topic_discovery_result", "data.connect.discovery.result")
	v.SetDefault("kafka.max_bytes", 10485760) // 10MB
	v.SetDefault("kafka.commit_interval", "1s")

	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.lock_ttl", "5m")
	v.SetDefault("redis.lock_retries", 3)

	v.SetDefault("postgres.max_open_conns", 10)
	v.SetDefault("postgres.max_idle_conns", 5)
	v.SetDefault("postgres.conn_max_lifetime", "1h")

	v.SetDefault("secret.provider", "env")
	v.SetDefault("secret.prefix", "SECRET_")

	shutdownTimeout, err := time.ParseDuration(v.GetString("app.shutdown_timeout"))
	if err != nil {
		return nil, fmt.Errorf("invalid shutdown_timeout: %w", err)
	}
	lockTTL, err := time.ParseDuration(v.GetString("redis.lock_ttl"))
	if err != nil {
		return nil, fmt.Errorf("invalid redis lock_ttl: %w", err)
	}
	commitInterval, err := time.ParseDuration(v.GetString("kafka.commit_interval"))
	if err != nil {
		return nil, fmt.Errorf("invalid kafka commit_interval: %w", err)
	}
	connMaxLifetime, err := time.ParseDuration(v.GetString("postgres.conn_max_lifetime"))
	if err != nil {
		return nil, fmt.Errorf("invalid postgres conn_max_lifetime: %w", err)
	}

	kafkaBrokers := v.GetStringSlice("kafka.brokers")
	if len(kafkaBrokers) == 0 {
		raw := v.GetString("kafka.brokers")
		if raw != "" {
			kafkaBrokers = strings.Split(raw, ",")
		}
	}

	cfg := &Config{
		App: AppConfig{
			Name:            v.GetString("app.name"),
			Environment:     v.GetString("app.environment"),
			LogLevel:        v.GetString("app.log_level"),
			ShutdownTimeout: shutdownTimeout,
		},
		Kafka: KafkaConfig{
			Brokers:              kafkaBrokers,
			GroupID:              v.GetString("kafka.group_id"),
			TopicSyncReq:         v.GetString("kafka.topic_sync_req"),
			TopicStarted:         v.GetString("kafka.topic_started"),
			TopicBatch:           v.GetString("kafka.topic_batch"),
			TopicCompleted:       v.GetString("kafka.topic_completed"),
			TopicFailed:          v.GetString("kafka.topic_failed"),
			TopicTestReq:         v.GetString("kafka.topic_test_req"),
			TopicDiscoveryReq:    v.GetString("kafka.topic_discovery_req"),
			TopicCheckpointReset: v.GetString("kafka.topic_checkpoint_reset"),
			TopicTestResult:      v.GetString("kafka.topic_test_result"),
			TopicDiscoveryResult: v.GetString("kafka.topic_discovery_result"),
			MaxBytes:             v.GetInt("kafka.max_bytes"),
			CommitInterval:       commitInterval,
		},
		Redis: RedisConfig{
			Addr:        v.GetString("redis.addr"),
			Password:    v.GetString("redis.password"),
			DB:          v.GetInt("redis.db"),
			LockTTL:     lockTTL,
			LockRetries: v.GetInt("redis.lock_retries"),
		},
		Postgres: PostgresConfig{
			DSN:             v.GetString("postgres.dsn"),
			MaxOpenConns:    v.GetInt("postgres.max_open_conns"),
			MaxIdleConns:    v.GetInt("postgres.max_idle_conns"),
			ConnMaxLifetime: connMaxLifetime,
		},
		Secret: SecretConfig{
			Provider: v.GetString("secret.provider"),
			Prefix:   v.GetString("secret.prefix"),
		},
	}

	return cfg, nil
}
