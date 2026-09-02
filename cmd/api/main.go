package api

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DrummDaddy/digital_store/internal/config"
	"github.com/DrummDaddy/digital_store/internal/database"
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := database.Open(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error(
			"databaase connection failed",
			"error",
			err,
		)
		os.Exit(1)
	}
	defer db.Close()
	logger.Info(
		"database connection succeeded",
		"address",
		cfg.HttpAddr,
	)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdownCtx.Ctx

	logger.Info("application sopped")

	if errors.Is(ctx.Err(), context.Canceled) {
		return
	}
}
