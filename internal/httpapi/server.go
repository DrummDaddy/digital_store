package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/DrummDaddy/digital_store/internal/model"
	"github.com/DrummDaddy/digital_store/internal/order"
)

type Server struct {
	orderRepo *order.Repository
	logger    *slog.Logger
}

func NewServer(
	orderRepo *order.Repository,
	logger *slog.Logger,
) *Server {
	return &Server{
		orderRepo: orderRepo,
		logger:    logger,
	}
}

func (s *Server) Router() http.Handler {
	r := chi.NewRouter()

	r.Post("/api/orders", s.createOrder)
	r.Get("/api/orders/{id}", s.getOrder)
	r.Post("/api/webhooks/payment", s.paymentWebhook)

	return r
}

func (s *Server) createOrder(
	w http.ResponseWriter,
	r *http.Request,
) {
	var request CreateOrderRequest

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(&request); err != nil {
		writeError(
			w,
			http.StatusBadRequest,
			"invalid JSON",
		)
		return
	}

	request.SKU = strings.TrimSpace(request.SKU)
	request.OrderID = strings.TrimSpace(request.OrderID)

	if request.SKU == "" {
		writeError(
			w,
			http.StatusBadRequest,
			"sku is required",
		)
		return
	}

	var (
		result model.Order
		err    error
	)

	if request.OrderID == "" {
		result, err = s.orderRepo.Create(
			r.Context(),
			request.SKU,
		)
	} else {
		orderID, parseErr := uuid.Parse(request.OrderID)
		if parseErr != nil {
			writeError(
				w,
				http.StatusBadRequest,
				"invalid order_id",
			)
			return
		}

		result, err = s.orderRepo.CreateWithID(
			r.Context(),
			orderID,
			request.SKU,
		)
	}

	if err != nil {
		switch {
		case errors.Is(err, order.ErrProductNotFound):
			writeError(
				w,
				http.StatusNotFound,
				"product not found",
			)

		default:
			s.logger.Error(
				"create order failed",
				"error", err,
				"order_id", request.OrderID,
				"sku", request.SKU,
			)

			writeError(
				w,
				http.StatusInternalServerError,
				"internal server error",
			)
		}

		return
	}

	writeJSON(
		w,
		http.StatusCreated,
		result,
	)
}

func (s *Server) getOrder(
	w http.ResponseWriter,
	r *http.Request,
) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid order id")
		return
	}

	result, err := s.orderRepo.GetByID(r.Context(), id)
	if err != nil {
		if errors.Is(err, order.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order not found")
			return
		}

		s.logger.Error(
			"get order failed",
			"error", err,
			"order_id", id,
		)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	writeJSON(w, http.StatusOK, result)
}

func (s *Server) paymentWebhook(
	w http.ResponseWriter,
	r *http.Request,
) {
	body := make([]byte, 0, 1024)

	decoder := json.NewDecoder(r.Body)

	var raw map[string]any

	if err := decoder.Decode(&raw); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	encoded, err := json.Marshal(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid payload")
		return
	}

	body = encoded

	var event model.PaymentWebhook

	if err := json.Unmarshal(body, &event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid payment payload")
		return
	}

	if event.EventID == "" {
		writeError(w, http.StatusBadRequest, "event_id is required")
		return
	}

	if event.OrderID == "" {
		writeError(w, http.StatusBadRequest, "order_id is required")
		return
	}

	if event.Status != "paid" && event.Status != "failed" {
		writeError(w, http.StatusBadRequest, "invalid payment status")
		return
	}

	if event.Amount < 0 {
		writeError(w, http.StatusBadRequest, "amount must not be negative")
		return
	}

	if event.Currency == "" {
		writeError(w, http.StatusBadRequest, "currency is required")
		return
	}

	if _, err := uuid.Parse(event.OrderID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid order_id")
		return
	}

	if err := s.orderRepo.ProcessPayment(
		r.Context(),
		event,
		body,
	); err != nil {
		s.logger.Error(
			"process payment webhook failed",
			"error", err,
			"event_id", event.EventID,
			"order_id", event.OrderID,
		)

		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	s.logger.Info(
		"payment webhook accepted",
		"event_id", event.EventID,
		"order_id", event.OrderID,
		"status", event.Status,
		"amount", event.Amount,
		"currency", event.Currency,
	)

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": true,
	})
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}

func writeError(
	w http.ResponseWriter,
	status int,
	message string,
) {
	writeJSON(w, status, map[string]string{
		"error": message,
	})
}
