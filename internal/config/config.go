package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr             string
	DatabaseURL          string
	DeliveryPollInterval time.Duration
	ProviderTimeout      time.Duration
	ProviderARate        float64
	ProviderATimeoutRate float64
	ProviderBRate        float64
	ProviderBTimeoutRate float64
}

func Load() Config {
	return Config{
		HTTPAddr: getEnv(
			"HTTP_ADDR",
			":8080",
		),
		DatabaseURL: getEnv(
			"DATABASE_URL",
			"postgres://digital_store:digital_store@localhost:5432/digital_store?sslmode=disable",
		),
		DeliveryPollInterval: getDurationEnv(
			"DELIVERY_POLL_INTERVAL",
			500*time.Millisecond,
		),
		ProviderTimeout: getDurationEnv(
			"PROVIDER_TIMEOUT",
			2*time.Second,
		),
		ProviderARate: getFloatEnv(
			"PROVIDER_A_FAIL_RATE",
			0,
		),
		ProviderATimeoutRate: getFloatEnv(
			"PROVIDER_A_TIMEOUT_RATE",
			0,
		),
		ProviderBRate: getFloatEnv(
			"PROVIDER_B_FAIL_RATE",
			0,
		),
		ProviderBTimeoutRate: getFloatEnv(
			"PROVIDER_B_TIMEOUT_RATE",
			0,
		),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	result, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}

	return result
}

func getFloatEnv(key string, fallback float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}

	if result < 0 {
		return fallback
	}

	if result > 1 {
		return fallback
	}

	return result
}
