package config

import (
	"errors"
	"os"
)

type Config struct {
	Port      string
	SinkURL   string
	SourceID  string
	EventType string
}

func Load() (*Config, error) {
	cfg := &Config{
		Port:      getEnv("PORT", "8080"),
		SinkURL:   getEnv("K_SINK", ""),
		SourceID:  getEnv("SOURCE_ID", "image-processing/webhook"),
		EventType: getEnv("EVENT_TYPE", "image.storage.requested"),
	}

	if cfg.Port == "" {
		return nil, errors.New("PORT environment variable must be set")
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return defaultValue
}
