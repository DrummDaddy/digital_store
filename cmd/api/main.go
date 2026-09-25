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

	"github.com/DrummDaddy/digital_store/internal/catalog"
	"github.com/DrummDaddy/digital_store/internal/config"
	"github.com/DrummDaddy/digital_store/internal/database"
	"github.com/DrummDaddy/digital_store/internal/delivery"
	"github.com/DrummDaddy/digital_store/internal/httpapi"
	"github.com/DrummDaddy/digital_store/internal/order"
	"github.com/DrummDaddy/digital_store/internal/reconciliation"
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

	db, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error(
			"database connection failed",
			"error", err,
		)
		os.Exit(1)
	}
	defer db.Close()

	catalogRepository := catalog.NewRepository(db)
	orderRepository := order.NewRepository(db)
	deliveryRepository := delivery.NewRepository(db)
	reconciliationService := reconciliation.NewService(db)

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

	apiServer := httpapi.NewServer(
		orderRepository,
		logger,
		httpapi.WithOperations(
			db,
			reconciliationService,
		),
		httpapi.WithCatalog(
			catalogRepository,
		),
	)

	httpServer := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           apiServer.Router(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverError := make(chan error, 1)
	workerError := make(chan error, 1)
	reconciliationError := make(chan error, 1)

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

	go func() {
		reconciliationError <- reconciliationService.RunRepairLoop(
			ctx,
			cfg.ReconciliationInterval,
		)
	}()

	select {
	case err := <-serverError:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error(
				"HTTP server failed",
				"error", err,
			)
		} else {
			logger.Info("HTTP server stopped")
		}

	case err := <-workerError:
		if err != nil {
			logger.Error(
				"delivery worker failed",
				"error", err,
			)
		} else {
			logger.Warn(
				"delivery worker stopped unexpectedly",
			)
		}

	case err := <-reconciliationError:
		if err != nil {
			logger.Error(
				"reconciliation loop failed",
				"error", err,
			)
		} else {
			logger.Warn(
				"reconciliation loop stopped unexpectedly",
			)
		}

	case <-ctx.Done():
		logger.Info(
			"shutdown signal received",
			"error", ctx.Err(),
		)
	}

	stop()

	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error(
			"HTTP server graceful shutdown failed",
			"error", err,
		)

		if closeErr := httpServer.Close(); closeErr != nil {
			logger.Error(
				"HTTP server forced close failed",
				"error", closeErr,
			)
		}
	}

	logger.Info("application stopped")
}
