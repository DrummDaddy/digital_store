package api

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

	api := httpapi.NewServer(
		orderRepository,
		logger,
		httpapi.WithOperations(
			db,
			reconciliationService,
		),
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

	case err := <-reconciliationError:
		if err != nil {
			logger.Error(
				"reconciliation loop failed",
				"error", err,
			)
		}

		stop()

	case <-ctx.Done():
		logger.Info("shutdown signal received")

		logger.Info("application stopped")
	}
}
