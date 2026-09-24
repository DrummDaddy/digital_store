package delivery

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	DeliveryID uuid.UUID
	OrderID    uuid.UUID
	RequestID  string
	SKU        string
	Attempts   int
}

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(
	db *pgxpool.Pool,
) *Repository {
	return &Repository{
		db: db,
	}
}

// ClaimNextJob атомарно выбирает одну задачу.
// SKIP LOCKED позволяет запускать несколько worker-ов.
func (r *Repository) ClaimNextJob(
	ctx context.Context,
) (Job, bool, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Job{}, false, fmt.Errorf(
			"begin claim transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var job Job

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			d.id,
			d.order_id,
			d.request_id,
			o.sku,
			d.attempts
		FROM delivery_attempts AS d
		JOIN orders AS o
			ON o.id = d.order_id
		WHERE d.status IN (
				'pending',
				'failed',
				'out_of_stock'
			)
		  AND (
				d.next_attempt_at IS NULL
				OR d.next_attempt_at <= now()
		  )
		  AND o.status IN (
				'paid',
				'delivery_failed',
				'out_of_stock'
		  )
		ORDER BY
			d.next_attempt_at NULLS FIRST,
			d.created_at
		FOR UPDATE OF d SKIP LOCKED
		LIMIT 1
		`,
	).Scan(
		&job.DeliveryID,
		&job.OrderID,
		&job.RequestID,
		&job.SKU,
		&job.Attempts,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return Job{}, false, fmt.Errorf(
					"commit empty claim transaction: %w",
					err,
				)
			}

			return Job{}, false, nil
		}

		return Job{}, false, fmt.Errorf(
			"select delivery job: %w",
			err,
		)
	}

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE delivery_attempts
		SET
			status = 'processing',
			attempts = attempts + 1,
			locked_at = now(),
			updated_at = now()
		WHERE id = $1
		  AND status IN (
				'pending',
				'failed',
				'out_of_stock'
		  )
		`,
		job.DeliveryID,
	)
	if err != nil {
		return Job{}, false, fmt.Errorf(
			"mark delivery processing: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return Job{}, false, errors.New(
			"delivery job was not claimed",
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = 'delivering',
			last_error = NULL,
			updated_at = now()
		WHERE id = $1
		  AND status IN (
				'paid',
				'delivery_failed',
				'out_of_stock'
		  )
		`,
		job.OrderID,
	)
	if err != nil {
		return Job{}, false, fmt.Errorf(
			"mark order delivering: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, fmt.Errorf(
			"commit claim transaction: %w",
			err,
		)
	}

	job.Attempts++

	return job, true, nil
}

func (r *Repository) MarkDelivered(
	ctx context.Context,
	job Job,
	provider string,
	code string,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin mark delivered transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var currentStatus string

	err = tx.QueryRow(
		ctx,
		`
		SELECT status
		FROM delivery_attempts
		WHERE id = $1
		FOR UPDATE
		`,
		job.DeliveryID,
	).Scan(&currentStatus)
	if err != nil {
		return fmt.Errorf(
			"lock delivery attempt: %w",
			err,
		)
	}

	if currentStatus == "delivered" {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf(
				"commit existing delivery: %w",
				err,
			)
		}

		return nil
	}

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE delivery_attempts
		SET
			provider = $2,
			status = 'delivered',
			code = $3,
			error = NULL,
			next_attempt_at = NULL,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1
		  AND status = 'processing'
		`,
		job.DeliveryID,
		provider,
		code,
	)
	if err != nil {
		return fmt.Errorf(
			"update delivery attempt: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return errors.New(
			"delivery attempt is not processing",
		)
	}

	commandTag, err = tx.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = 'delivered',
			delivered_at = COALESCE(delivered_at, now()),
			last_error = NULL,
			updated_at = now()
		WHERE id = $1
		  AND status = 'delivering'
		`,
		job.OrderID,
	)
	if err != nil {
		return fmt.Errorf(
			"mark order delivered: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return errors.New(
			"order is not in delivering status",
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO ledger_entries (
			order_id,
			entry_type,
			amount_minor,
			currency
		)
		SELECT
			id,
			'delivery',
			0,
			currency
		FROM orders
		WHERE id = $1
		  AND NOT EXISTS (
				SELECT 1
				FROM ledger_entries
				WHERE order_id = $1
				  AND entry_type = 'delivery'
		  )
		`,
		job.OrderID,
	)
	if err != nil {
		return fmt.Errorf(
			"insert delivery ledger entry: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit delivered transaction: %w",
			err,
		)
	}

	return nil
}

func (r *Repository) MarkOutOfStock(
	ctx context.Context,
	job Job,
	message string,
	retryDelay time.Duration,
) error {
	return r.markRecoverableFailure(
		ctx,
		job,
		"out_of_stock",
		"out_of_stock",
		message,
		retryDelay,
	)
}

func (r *Repository) MarkFailed(
	ctx context.Context,
	job Job,
	message string,
	retryDelay time.Duration,
) error {
	return r.markRecoverableFailure(
		ctx,
		job,
		"failed",
		"delivery_failed",
		message,
		retryDelay,
	)
}

func (r *Repository) markRecoverableFailure(
	ctx context.Context,
	job Job,
	deliveryStatus string,
	orderStatus string,
	message string,
	retryDelay time.Duration,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin delivery failure transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	nextAttemptAt := time.Now().UTC().Add(retryDelay)

	commandTag, err := tx.Exec(
		ctx,
		`
		UPDATE delivery_attempts
		SET
			status = $2,
			error = $3,
			next_attempt_at = $4,
			locked_at = NULL,
			updated_at = now()
		WHERE id = $1
		  AND status = 'processing'
		`,
		job.DeliveryID,
		deliveryStatus,
		message,
		nextAttemptAt,
	)
	if err != nil {
		return fmt.Errorf(
			"mark delivery recoverable failure: %w",
			err,
		)
	}

	if commandTag.RowsAffected() != 1 {
		return errors.New(
			"delivery attempt is not processing",
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = $2::order_status,
			last_error = $3,
			updated_at = now()
		WHERE id = $1
		  AND status = 'delivering'
		`,
		job.OrderID,
		orderStatus,
		message,
	)
	if err != nil {
		return fmt.Errorf(
			"mark order recoverable failure: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit delivery failure transaction: %w",
			err,
		)
	}

	return nil
}

// RecoverStaleJobs возвращает processing-задачи после падения worker-а.
func (r *Repository) RecoverStaleJobs(
	ctx context.Context,
	staleAfter time.Duration,
) (int64, error) {
	cutoff := time.Now().UTC().Add(-staleAfter)

	commandTag, err := r.db.Exec(
		ctx,
		`
		WITH recovered AS (
			UPDATE delivery_attempts
			SET
				status = 'failed',
				error = 'worker lease expired',
				next_attempt_at = now(),
				locked_at = NULL,
				updated_at = now()
			WHERE status = 'processing'
			  AND locked_at < $1
			RETURNING order_id
		)
		UPDATE orders AS o
		SET
			status = 'delivery_failed',
			last_error = 'worker lease expired',
			updated_at = now()
		FROM recovered AS r
		WHERE o.id = r.order_id
		  AND o.status = 'delivering'
		`,
		cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf(
			"recover stale jobs: %w",
			err,
		)
	}

	return commandTag.RowsAffected(), nil
}
