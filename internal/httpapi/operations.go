package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/DrummDaddy/digital_store/internal/reconciliation"
)

func (s *Server) liveness(
	w http.ResponseWriter,
	_ *http.Request,
) {
	writeJSON(
		w,
		http.StatusOK,
		map[string]any{
			"status": "ok",
		},
	)
}

func (s *Server) readiness(
	w http.ResponseWriter,
	r *http.Request,
) {
	if s.db == nil {
		writeJSON(
			w,
			http.StatusServiceUnavailable,
			map[string]any{
				"status": "not_ready",
				"error":  "database dependency is not configured",
			},
		)
		return
	}

	ctx, cancel := context.WithTimeout(
		r.Context(),
		2*time.Second,
	)
	defer cancel()

	if err := s.db.Ping(ctx); err != nil {
		s.logger.Error(
			"readiness database check failed",
			"error", err,
		)

		writeJSON(
			w,
			http.StatusServiceUnavailable,
			map[string]any{
				"status": "not_ready",
				"error":  "database is unavailable",
			},
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		map[string]any{
			"status":   "ready",
			"database": "ok",
		},
	)
}

func (s *Server) reconciliationReport(
	w http.ResponseWriter,
	r *http.Request,
) {
	if s.reconciliationService == nil {
		writeError(
			w,
			http.StatusServiceUnavailable,
			"reconciliation service is not configured",
		)
		return
	}

	report, err := s.reconciliationService.BuildReport(
		r.Context(),
	)
	if err != nil {
		s.logger.Error(
			"build reconciliation report failed",
			"error", err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"internal server error",
		)
		return
	}

	writeJSON(
		w,
		http.StatusOK,
		report,
	)
}

func (s *Server) repairReconciliation(
	w http.ResponseWriter,
	r *http.Request,
) {
	if s.reconciliationService == nil {
		writeError(
			w,
			http.StatusServiceUnavailable,
			"reconciliation service is not configured",
		)
		return
	}

	result, err := s.reconciliationService.Repair(
		r.Context(),
	)
	if err != nil {
		s.logger.Error(
			"reconciliation repair failed",
			"error", err,
		)

		writeError(
			w,
			http.StatusInternalServerError,
			"internal server error",
		)
		return
	}

	s.logger.Info(
		"reconciliation repair completed",
		"created_delivery_jobs",
		result.CreatedDeliveryJobs,
		"queued_recoverable",
		result.QueuedRecoverable,
	)

	writeJSON(
		w,
		http.StatusOK,
		result,
	)
}

func (s *Server) retryDelivery(
	w http.ResponseWriter,
	r *http.Request,
) {
	if s.reconciliationService == nil {
		writeError(
			w,
			http.StatusServiceUnavailable,
			"reconciliation service is not configured",
		)
		return
	}

	orderID, err := uuid.Parse(
		chi.URLParam(r, "id"),
	)
	if err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid order id",
		)
		return
	}

	result, err := s.reconciliationService.RetryDelivery(
		r.Context(),
		orderID,
	)
	if err != nil {
		switch {
		case errors.Is(
			err,
			reconciliation.ErrOrderNotFound,
		):
			writeError(
				w,
				http.StatusNotFound,
				"order not found",
			)

		case errors.Is(
			err,
			reconciliation.ErrOrderNotPaid,
		):
			writeError(
				w,
				http.StatusConflict,
				"order is not paid",
			)

		case errors.Is(
			err,
			reconciliation.ErrDeliveryRunning,
		):
			writeError(
				w,
				http.StatusConflict,
				"delivery is currently processing",
			)

		default:
			s.logger.Error(
				"retry delivery failed",
				"error", err,
				"order_id", orderID,
			)

			writeError(
				w,
				http.StatusInternalServerError,
				"internal server error",
			)
		}

		return
	}

	s.logger.Info(
		"delivery retry requested",
		"order_id", orderID,
		"status", result.Status,
	)

	writeJSON(
		w,
		http.StatusOK,
		result,
	)
}
