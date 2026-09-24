package delivery

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type Worker struct {
	repository   *Repository
	service      *Service
	pollInterval time.Duration
	logger       *slog.Logger
}

func NewWorker(
	repository *Repository,
	service *Service,
	pollInterval time.Duration,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		repository:   repository,
		service:      service,
		pollInterval: pollInterval,
		logger:       logger,
	}
}

func (w *Worker) Run(
	ctx context.Context,
) error {
	w.logger.Info(
		"delivery worker started",
		"poll_interval", w.pollInterval.String(),
	)

	recoveryTicker := time.NewTicker(30 * time.Second)
	defer recoveryTicker.Stop()

	for {
		processed, err := w.processNext(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			w.logger.Error(
				"delivery worker iteration failed",
				"error", err,
			)
		}

		if processed {
			// Сразу пробуем забрать следующую задачу,
			// не ждём poll interval.
			continue
		}

		timer := time.NewTimer(w.pollInterval)

		select {
		case <-ctx.Done():
			timer.Stop()
			w.logger.Info("delivery worker stopped")
			return nil

		case <-recoveryTicker.C:
			timer.Stop()

			count, err := w.repository.RecoverStaleJobs(
				ctx,
				2*time.Minute,
			)
			if err != nil {
				w.logger.Error(
					"stale job recovery failed",
					"error", err,
				)
			} else if count > 0 {
				w.logger.Warn(
					"stale jobs recovered",
					"count", count,
				)
			}

		case <-timer.C:
		}
	}
}

func (w *Worker) processNext(
	ctx context.Context,
) (bool, error) {
	job, found, err := w.repository.ClaimNextJob(ctx)
	if err != nil {
		return false, err
	}

	if !found {
		return false, nil
	}

	w.logger.Info(
		"delivery job claimed",
		"delivery_id", job.DeliveryID,
		"order_id", job.OrderID,
		"request_id", job.RequestID,
		"attempt", job.Attempts,
	)

	if err := w.service.Process(ctx, job); err != nil {
		return true, err
	}

	return true, nil
}
