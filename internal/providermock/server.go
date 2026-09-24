package providermock

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DrummDaddy/digital_store/internal/delivery"
)

type Behavior struct {
	FailRate    float64
	TimeoutRate float64
	Timeout     time.Duration
}

type Server struct {
	db *pgxpool.Pool

	providerA Behavior
	providerB Behavior

	randomMu sync.Mutex
	random   *rand.Rand

	logger *slog.Logger
}

func NewServer(
	db *pgxpool.Pool,
	providerA Behavior,
	providerB Behavior,
	logger *slog.Logger,
) *Server {
	return &Server{
		db:        db,
		providerA: providerA,
		providerB: providerB,
		random: rand.New(
			rand.NewSource(time.Now().UnixNano()),
		),
		logger: logger,
	}
}

func (s *Server) Router() http.Handler {
	router := chi.NewRouter()

	router.Post(
		"/provider-a/issue",
		s.issue("provider_a", s.providerA),
	)

	router.Post(
		"/provider-b/issue",
		s.issue("provider_b", s.providerB),
	)

	router.Get(
		"/health",
		func(w http.ResponseWriter, _ *http.Request) {
			writeJSON(
				w,
				http.StatusOK,
				map[string]bool{
					"ok": true,
				},
			)
		},
	)

	return router
}

func (s *Server) issue(
	provider string,
	behavior Behavior,
) http.HandlerFunc {
	return func(
		w http.ResponseWriter,
		r *http.Request,
	) {
		var request delivery.IssueRequest

		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()

		if err := decoder.Decode(&request); err != nil {
			writeJSON(
				w,
				http.StatusBadRequest,
				delivery.IssueResponse{
					Status: "error",
					Reason: "invalid_request",
				},
			)
			return
		}

		if request.RequestID == "" ||
			request.SKU == "" ||
			request.OrderID == "" {
			writeJSON(
				w,
				http.StatusBadRequest,
				delivery.IssueResponse{
					Status: "error",
					Reason: "invalid_request",
				},
			)
			return
		}

		orderID, err := uuid.Parse(request.OrderID)
		if err != nil {
			writeJSON(
				w,
				http.StatusBadRequest,
				delivery.IssueResponse{
					Status: "error",
					Reason: "invalid_order_id",
				},
			)
			return
		}

		result, timeout, err := s.issueCode(
			r.Context(),
			provider,
			behavior,
			request,
			orderID,
		)
		if err != nil {
			switch {
			case errors.Is(err, delivery.ErrOutOfStock):
				writeJSON(
					w,
					http.StatusConflict,
					delivery.IssueResponse{
						Status: "error",
						Reason: "out_of_stock",
					},
				)

			case errors.Is(
				err,
				delivery.ErrProviderUnavailable,
			):
				writeJSON(
					w,
					http.StatusServiceUnavailable,
					delivery.IssueResponse{
						Status: "error",
						Reason: "provider_unavailable",
					},
				)

			default:
				s.logger.Error(
					"mock provider issue failed",
					"provider", provider,
					"request_id", request.RequestID,
					"error", err,
				)

				writeJSON(
					w,
					http.StatusInternalServerError,
					delivery.IssueResponse{
						Status: "error",
						Reason: "internal_error",
					},
				)
			}

			return
		}

		if timeout {
			// Код уже записан в БД и считается выданным.
			// Ответ специально задерживается дольше клиентского
			// timeout. Повтор с тем же request_id вернёт тот же код.
			time.Sleep(behavior.Timeout)
		}

		writeJSON(
			w,
			http.StatusOK,
			result,
		)
	}
}

func (s *Server) issueCode(
	ctx context.Context,
	provider string,
	behavior Behavior,
	request delivery.IssueRequest,
	orderID uuid.UUID,
) (delivery.IssueResponse, bool, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return delivery.IssueResponse{}, false, err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	// Сериализуем параллельные запросы с одинаковыми
	// provider + request_id.
	_, err = tx.Exec(
		ctx,
		`
		SELECT pg_advisory_xact_lock(
			hashtextextended($1, 0)
		)
		`,
		provider+":"+request.RequestID,
	)
	if err != nil {
		return delivery.IssueResponse{}, false, err
	}

	// Идемпотентность: если request_id уже был обработан,
	// возвращаем ранее выданный код.
	var existingCode string

	err = tx.QueryRow(
		ctx,
		`
		SELECT code
		FROM provider_issues
		WHERE provider = $1
		  AND request_id = $2
		  AND status = 'issued'
		`,
		provider,
		request.RequestID,
	).Scan(&existingCode)
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return delivery.IssueResponse{}, false, err
		}

		return delivery.IssueResponse{
			Status:    "ok",
			RequestID: request.RequestID,
			Code:      existingCode,
		}, false, nil
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return delivery.IssueResponse{}, false, err
	}

	// Явный 5xx происходит до выдачи кода.
	// Поэтому после такого ответа безопасен fallback.
	if s.randomFloat() < behavior.FailRate {
		return delivery.IssueResponse{},
			false,
			delivery.ErrProviderUnavailable
	}

	timeout := s.randomFloat() < behavior.TimeoutRate

	var (
		keyID uuid.UUID
		code  string
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			code
		FROM provider_keys
		WHERE provider = $1
		  AND sku = $2
		  AND issued = FALSE
		ORDER BY created_at, id
		FOR UPDATE SKIP LOCKED
		LIMIT 1
		`,
		provider,
		request.SKU,
	).Scan(
		&keyID,
		&code,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return delivery.IssueResponse{},
				false,
				delivery.ErrOutOfStock
		}

		return delivery.IssueResponse{}, false, err
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE provider_keys
		SET
			issued = TRUE,
			request_id = $2,
			order_id = $3,
			issued_at = now()
		WHERE id = $1
		  AND issued = FALSE
		`,
		keyID,
		request.RequestID,
		orderID,
	)
	if err != nil {
		return delivery.IssueResponse{}, false, err
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO provider_issues (
			provider,
			request_id,
			sku,
			order_id,
			code,
			status
		)
		VALUES (
			$1,
			$2,
			$3,
			$4,
			$5,
			'issued'
		)
		`,
		provider,
		request.RequestID,
		request.SKU,
		orderID,
		code,
	)
	if err != nil {
		return delivery.IssueResponse{}, false, err
	}

	if err := tx.Commit(ctx); err != nil {
		return delivery.IssueResponse{}, false, err
	}

	s.logger.Info(
		"mock provider issued code",
		"provider", provider,
		"request_id", request.RequestID,
		"order_id", request.OrderID,
		"sku", request.SKU,
		"timeout_after_issue", timeout,
	)

	return delivery.IssueResponse{
		Status:    "ok",
		RequestID: request.RequestID,
		Code:      code,
	}, timeout, nil
}

func (s *Server) randomFloat() float64 {
	s.randomMu.Lock()
	defer s.randomMu.Unlock()

	return s.random.Float64()
}

func writeJSON(
	w http.ResponseWriter,
	status int,
	value any,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(value)
}
