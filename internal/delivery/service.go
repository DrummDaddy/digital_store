package delivery

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type Service struct {
	repository *Repository

	providerA Provider
	providerB Provider

	retryCount int
	retryDelay time.Duration

	logger *slog.Logger
}

func NewService(
	repository *Repository,
	providerA Provider,
	providerB Provider,
	retryCount int,
	retryDelay time.Duration,
	logger *slog.Logger,
) *Service {
	if retryCount < 1 {
		retryCount = 1
	}

	return &Service{
		repository: repository,
		providerA:  providerA,
		providerB:  providerB,
		retryCount: retryCount,
		retryDelay: retryDelay,
		logger:     logger,
	}
}

func (s *Service) Process(
	ctx context.Context,
	job Job,
) error {
	request := IssueRequest{
		RequestID: job.RequestID,
		SKU:       job.SKU,
		OrderID:   job.OrderID.String(),
	}

	startedAt := time.Now()

	response, err := s.issueWithRetry(
		ctx,
		s.providerA,
		request,
	)

	if err == nil {
		return s.complete(
			ctx,
			job,
			s.providerA.Name(),
			response.Code,
			startedAt,
		)
	}

	// Таймаут — неопределённый результат.
	// A мог уже выдать код. Поэтому B вызывать нельзя.
	if errors.Is(err, ErrProviderTimeout) {
		return s.fail(
			ctx,
			job,
			fmt.Sprintf(
				"provider %s timeout with unknown result",
				s.providerA.Name(),
			),
		)
	}

	// На явном 5xx или out_of_stock можно перейти к B.
	if !errors.Is(err, ErrProviderUnavailable) &&
		!errors.Is(err, ErrOutOfStock) {
		return s.fail(
			ctx,
			job,
			err.Error(),
		)
	}

	s.logger.Warn(
		"fallback to provider B",
		"order_id", job.OrderID,
		"request_id", job.RequestID,
		"provider_a_error", err,
	)

	response, fallbackErr := s.issueWithRetry(
		ctx,
		s.providerB,
		request,
	)
	if fallbackErr == nil {
		return s.complete(
			ctx,
			job,
			s.providerB.Name(),
			response.Code,
			startedAt,
		)
	}

	if errors.Is(fallbackErr, ErrOutOfStock) &&
		errors.Is(err, ErrOutOfStock) {
		return s.outOfStock(
			ctx,
			job,
			"all providers are out of stock",
		)
	}

	if errors.Is(fallbackErr, ErrProviderTimeout) {
		return s.fail(
			ctx,
			job,
			fmt.Sprintf(
				"provider %s timeout with unknown result",
				s.providerB.Name(),
			),
		)
	}

	return s.fail(
		ctx,
		job,
		fmt.Sprintf(
			"providers failed: A=%v; B=%v",
			err,
			fallbackErr,
		),
	)
}

func (s *Service) issueWithRetry(
	ctx context.Context,
	provider Provider,
	request IssueRequest,
) (IssueResponse, error) {
	var lastErr error

	for attempt := 1; attempt <= s.retryCount; attempt++ {
		startedAt := time.Now()

		response, err := provider.Issue(
			ctx,
			request,
		)

		s.logger.Info(
			"provider issue attempt",
			"provider", provider.Name(),
			"order_id", request.OrderID,
			"request_id", request.RequestID,
			"sku", request.SKU,
			"attempt", attempt,
			"duration_ms", time.Since(startedAt).Milliseconds(),
			"error", err,
		)

		if err == nil {
			return response, nil
		}

		lastErr = err

		if errors.Is(err, ErrOutOfStock) {
			return IssueResponse{}, err
		}

		if errors.Is(err, ErrInvalidResponse) {
			return IssueResponse{}, err
		}

		if attempt == s.retryCount {
			break
		}

		backoff := retryBackoff(attempt)

		timer := time.NewTimer(backoff)

		select {
		case <-ctx.Done():
			timer.Stop()
			return IssueResponse{}, ctx.Err()

		case <-timer.C:
		}
	}

	return IssueResponse{}, lastErr
}

func retryBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 100 * time.Millisecond
	case 2:
		return 300 * time.Millisecond
	default:
		return 900 * time.Millisecond
	}
}

func (s *Service) complete(
	ctx context.Context,
	job Job,
	provider string,
	code string,
	startedAt time.Time,
) error {
	if err := s.repository.MarkDelivered(
		ctx,
		job,
		provider,
		code,
	); err != nil {
		return fmt.Errorf(
			"mark delivered: %w",
			err,
		)
	}

	s.logger.Info(
		"delivery completed",
		"order_id", job.OrderID,
		"request_id", job.RequestID,
		"provider", provider,
		"attempt", job.Attempts,
		"duration_ms", time.Since(startedAt).Milliseconds(),
	)

	return nil
}

func (s *Service) outOfStock(
	ctx context.Context,
	job Job,
	message string,
) error {
	if err := s.repository.MarkOutOfStock(
		ctx,
		job,
		message,
		s.retryDelay,
	); err != nil {
		return fmt.Errorf(
			"mark out of stock: %w",
			err,
		)
	}

	s.logger.Warn(
		"delivery out of stock",
		"order_id", job.OrderID,
		"request_id", job.RequestID,
		"error", message,
	)

	return nil
}

func (s *Service) fail(
	ctx context.Context,
	job Job,
	message string,
) error {
	if err := s.repository.MarkFailed(
		ctx,
		job,
		message,
		s.retryDelay,
	); err != nil {
		return fmt.Errorf(
			"mark delivery failed: %w",
			err,
		)
	}

	s.logger.Error(
		"delivery failed",
		"order_id", job.OrderID,
		"request_id", job.RequestID,
		"error", message,
	)

	return nil
}
