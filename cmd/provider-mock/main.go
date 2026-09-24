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
	"github.com/DrummDaddy/digital_store/internal/delivery"
	"github.com/DrummDaddy/digital_store/internal/httpapi"
	"github.com/DrummDaddy/digital_store/internal/order"
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

	orderRepository := order.NewRepository(db)

	deliveryRepository := delivery.NewRepository(db)

	providerA := delivery.NewHTTPProvider(
		"provider_a",
		cfg.ProviderAURL,
		cfg.ProviderTimeout,
	)

	providerB := delivery.NewHTTPProvider(
		"provider_b",
		cfg.ProviderBURL,
		cfg.ProviderTimeout,
	)

	deliveryService := delivery.NewService(
		deliveryRepository,
		providerA,
		providerB,
		cfg.ProviderRetryCount,
		cfg.DeliveryRetryDelay,
		logger,
	)

	deliveryWorker := delivery.NewWorker(
		deliveryRepository,
		deliveryService,
		cfg.DeliveryPollInterval,
		logger,
	)

	api := httpapi.NewServer(
		orderRepository,
		logger,
	)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	workerError := make(chan error, 1)

	go func() {
		logger.Info(
			"HTTP server started",
			"address", cfg.HTTPAddr,
		)

		serverError <- httpServer.ListenAndServe()
	}()

	go func() {
		workerError <- deliveryWorker.Run(ctx)
	}()

	select {
	case err := <-serverError:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error(
				"HTTP server failed",
				"error", err,
			)
		}

		stop()

	case err := <-workerError:
		if err != nil {
			logger.Error(
				"delivery worker failed",
				"error", err,
			)
		}

		stop()

	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(
			"HTTP server shutdown failed",
			"error", err,
		)
	}

	logger.Info("application stopped")
}
