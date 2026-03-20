package config

import (
	"fmt"
	"os"
	"strings"
)

// SecretLoader resolves secret references to actual values
type SecretLoader struct {
	cfg SecretConfig
}

// NewSecretLoader creates a new SecretLoader
func NewSecretLoader(cfg SecretConfig) *SecretLoader {
	return &SecretLoader{cfg: cfg}
}

// Resolve looks up the secret value for the given reference
// passwordRef is a key like "DB_PASSWORD_SOURCE_ABC" pointing to an env var or secret path
func (s *SecretLoader) Resolve(passwordRef string) (string, error) {
	switch s.cfg.Provider {
	case "env":
		return s.resolveFromEnv(passwordRef)
	// Future: case "gcp": return s.resolveFromGCP(passwordRef)
	// Future: case "vault": return s.resolveFromVault(passwordRef)
	default:
		return "", fmt.Errorf("unsupported secret provider: %s", s.cfg.Provider)
	}
}

func (s *SecretLoader) resolveFromEnv(ref string) (string, error) {
	// Accept either a direct env var name or a prefixed reference
	envKey := ref
	if !strings.HasPrefix(ref, s.cfg.Prefix) {
		envKey = s.cfg.Prefix + ref
	}
	val := os.Getenv(envKey)
	if val == "" {
		// Also try the ref as-is without prefix
		val = os.Getenv(ref)
	}
	if val == "" {
		return "", fmt.Errorf("secret not found for ref %q (tried env vars %q and %q)", ref, envKey, ref)
	}
	return val, nil
}
