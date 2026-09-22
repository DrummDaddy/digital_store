package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	HTTPAddr    string
	DatabaseURL string

	DeliveryPollInterval time.Duration
	DeliveryRetryDelay   time.Duration
	ProviderTimeout      time.Duration
	ProviderRetryCount   int

	ProviderAURL string
	ProviderBURL string

	ProviderMockAddr string

	ProviderAFailRate    float64
	ProviderATimeoutRate float64
	ProviderBFailRate    float64
	ProviderBTimeoutRate float64

	ProviderMockTimeout time.Duration
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
		DeliveryRetryDelay: getDurationEnv(
			"DELIVERY_RETRY_DELAY",
			10*time.Second,
		),
		ProviderTimeout: getDurationEnv(
			"PROVIDER_TIMEOUT",
			2*time.Second,
		),
		ProviderRetryCount: getIntEnv(
			"PROVIDER_RETRY_COUNT",
			3,
		),

		ProviderAURL: getEnv(
			"PROVIDER_A_URL",
			"http://localhost:8081/provider-a",
		),
		ProviderBURL: getEnv(
			"PROVIDER_B_URL",
			"http://localhost:8081/provider-b",
		),

		ProviderMockAddr: getEnv(
			"PROVIDER_MOCK_ADDR",
			":8081",
		),

		ProviderAFailRate: getFloatEnv(
			"PROVIDER_A_FAIL_RATE",
			0,
		),
		ProviderATimeoutRate: getFloatEnv(
			"PROVIDER_A_TIMEOUT_RATE",
			0,
		),
		ProviderBFailRate: getFloatEnv(
			"PROVIDER_B_FAIL_RATE",
			0,
		),
		ProviderBTimeoutRate: getFloatEnv(
			"PROVIDER_B_TIMEOUT_RATE",
			0,
		),
		ProviderMockTimeout: getDurationEnv(
			"PROVIDER_MOCK_TIMEOUT",
			5*time.Second,
		),
	}
}

func getEnv(key string, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func getDurationEnv(
	key string,
	fallback time.Duration,
) time.Duration {
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

func getFloatEnv(
	key string,
	fallback float64,
) float64 {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}

	if result < 0 || result > 1 {
		return fallback
	}

	return result
}

func getIntEnv(
	key string,
	fallback int,
) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	result, err := strconv.Atoi(value)
	if err != nil || result < 1 {
		return fallback
	}

	return result
}
