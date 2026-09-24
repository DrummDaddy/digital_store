package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DrummDaddy/digital_store/internal/config"
	"github.com/DrummDaddy/digital_store/internal/database"
	"github.com/DrummDaddy/digital_store/internal/providermock"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	db, err := database.Open(
		ctx,
		cfg.DatabaseURL,
	)
	if err != nil {
		logger.Error(
			"database connection failed",
			"error", err,
		)
		os.Exit(1)
	}
	defer db.Close()

	mockServer := providermock.NewServer(
		db,
		providermock.Behavior{
			FailRate:    cfg.ProviderAFailRate,
			TimeoutRate: cfg.ProviderATimeoutRate,
			Timeout:     cfg.ProviderMockTimeout,
		},
		providermock.Behavior{
			FailRate:    cfg.ProviderBFailRate,
			TimeoutRate: cfg.ProviderBTimeoutRate,
			Timeout:     cfg.ProviderMockTimeout,
		},
		logger,
	)

	httpServer := &http.Server{
		Addr:              cfg.ProviderMockAddr,
		Handler:           mockServer.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)

	go func() {
		logger.Info(
			"provider mock started",
			"address", cfg.ProviderMockAddr,
			"provider_a_fail_rate",
			cfg.ProviderAFailRate,
			"provider_a_timeout_rate",
			cfg.ProviderATimeoutRate,
			"provider_b_fail_rate",
			cfg.ProviderBFailRate,
			"provider_b_timeout_rate",
			cfg.ProviderBTimeoutRate,
		)

		serverError <- httpServer.ListenAndServe()
	}()

	select {
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error(
				"provider mock failed",
				"error", err,
			)
			os.Exit(1)
		}

	case <-ctx.Done():
		logger.Info(
			"provider mock shutdown signal received",
		)
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(
			"provider mock shutdown failed",
			"error", err,
		)
	}
}
