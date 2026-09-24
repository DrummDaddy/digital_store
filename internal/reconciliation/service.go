package reconciliation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrOrderNotFound   = errors.New("order not found")
	ErrOrderNotPaid    = errors.New("order is not paid")
	ErrDeliveryRunning = errors.New("delivery is currently processing")
)

type Service struct {
	db *pgxpool.Pool
}

func NewService(
	db *pgxpool.Pool,
) *Service {
	return &Service{
		db: db,
	}
}

func (s *Service) BuildReport(
	ctx context.Context,
) (Report, error) {
	report := Report{
		GeneratedAt:      time.Now().UTC(),
		PaidNotDelivered: make([]Issue, 0),
		DeliveredNotPaid: make([]Issue, 0),
		StatusMismatches: make([]Issue, 0),
		LedgerMismatches: make([]Issue, 0),
	}

	var err error

	report.PaidNotDelivered, err =
		s.findPaidNotDelivered(ctx)
	if err != nil {
		return Report{}, err
	}

	report.DeliveredNotPaid, err =
		s.findDeliveredNotPaid(ctx)
	if err != nil {
		return Report{}, err
	}

	report.StatusMismatches, err =
		s.findStatusMismatches(ctx)
	if err != nil {
		return Report{}, err
	}

	report.LedgerMismatches, err =
		s.findLedgerMismatches(ctx)
	if err != nil {
		return Report{}, err
	}

	report.Summary = Summary{
		PaidNotDelivered: len(report.PaidNotDelivered),
		DeliveredNotPaid: len(report.DeliveredNotPaid),
		StatusMismatches: len(report.StatusMismatches),
		LedgerMismatches: len(report.LedgerMismatches),
	}

	report.Summary.Total =
		report.Summary.PaidNotDelivered +
			report.Summary.DeliveredNotPaid +
			report.Summary.StatusMismatches +
			report.Summary.LedgerMismatches

	return report, nil
}

func (s *Service) findPaidNotDelivered(
	ctx context.Context,
) ([]Issue, error) {
	rows, err := s.db.Query(
		ctx,
		`
		SELECT
			o.id,
			o.sku,
			o.status::text,
			d.status,
			d.request_id,
			o.paid_at,
			o.delivered_at,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'payment'
			) AS payment_ledger_entries,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'delivery'
			) AS delivery_ledger_entries
		FROM orders AS o
		LEFT JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE (
				o.paid_at IS NOT NULL
				OR EXISTS (
					SELECT 1
					FROM ledger_entries AS payment
					WHERE payment.order_id = o.id
					  AND payment.entry_type = 'payment'
				)
			)
		  AND (
				d.id IS NULL
				OR d.status <> 'delivered'
		  )
		ORDER BY o.created_at, o.id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query paid not delivered orders: %w",
			err,
		)
	}
	defer rows.Close()

	issues := make([]Issue, 0)

	for rows.Next() {
		var issue Issue

		if err := rows.Scan(
			&issue.OrderID,
			&issue.SKU,
			&issue.OrderStatus,
			&issue.DeliveryStatus,
			&issue.RequestID,
			&issue.PaidAt,
			&issue.DeliveredAt,
			&issue.PaymentLedgerEntries,
			&issue.DeliveryLedgerEntries,
		); err != nil {
			return nil, fmt.Errorf(
				"scan paid not delivered order: %w",
				err,
			)
		}

		issue.Type = IssuePaidNotDelivered
		issue.Description =
			"payment is confirmed, but product is not delivered"

		issues = append(issues, issue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate paid not delivered orders: %w",
			err,
		)
	}

	return issues, nil
}

func (s *Service) findDeliveredNotPaid(
	ctx context.Context,
) ([]Issue, error) {
	rows, err := s.db.Query(
		ctx,
		`
		SELECT
			o.id,
			o.sku,
			o.status::text,
			d.status,
			d.request_id,
			o.paid_at,
			o.delivered_at,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'payment'
			) AS payment_ledger_entries,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'delivery'
			) AS delivery_ledger_entries
		FROM orders AS o
		JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE d.status = 'delivered'
		  AND (
				o.paid_at IS NULL
				OR NOT EXISTS (
					SELECT 1
					FROM ledger_entries AS payment
					WHERE payment.order_id = o.id
					  AND payment.entry_type = 'payment'
				)
		  )
		ORDER BY o.created_at, o.id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query delivered not paid orders: %w",
			err,
		)
	}
	defer rows.Close()

	issues := make([]Issue, 0)

	for rows.Next() {
		var issue Issue

		if err := rows.Scan(
			&issue.OrderID,
			&issue.SKU,
			&issue.OrderStatus,
			&issue.DeliveryStatus,
			&issue.RequestID,
			&issue.PaidAt,
			&issue.DeliveredAt,
			&issue.PaymentLedgerEntries,
			&issue.DeliveryLedgerEntries,
		); err != nil {
			return nil, fmt.Errorf(
				"scan delivered not paid order: %w",
				err,
			)
		}

		issue.Type = IssueDeliveredNotPaid
		issue.Description =
			"product is delivered, but payment confirmation is missing"

		issues = append(issues, issue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate delivered not paid orders: %w",
			err,
		)
	}

	return issues, nil
}

func (s *Service) findStatusMismatches(
	ctx context.Context,
) ([]Issue, error) {
	rows, err := s.db.Query(
		ctx,
		`
		SELECT
			o.id,
			o.sku,
			o.status::text,
			d.status,
			d.request_id,
			o.paid_at,
			o.delivered_at,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'payment'
			),
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'delivery'
			)
		FROM orders AS o
		JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE (
				o.status = 'delivered'
				AND d.status <> 'delivered'
			)
		   OR (
				o.status <> 'delivered'
				AND d.status = 'delivered'
			)
		ORDER BY o.created_at, o.id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query status mismatches: %w",
			err,
		)
	}
	defer rows.Close()

	issues := make([]Issue, 0)

	for rows.Next() {
		var issue Issue

		if err := rows.Scan(
			&issue.OrderID,
			&issue.SKU,
			&issue.OrderStatus,
			&issue.DeliveryStatus,
			&issue.RequestID,
			&issue.PaidAt,
			&issue.DeliveredAt,
			&issue.PaymentLedgerEntries,
			&issue.DeliveryLedgerEntries,
		); err != nil {
			return nil, fmt.Errorf(
				"scan status mismatch: %w",
				err,
			)
		}

		issue.Type = IssueOrderMismatch
		issue.Description =
			"order and delivery statuses do not match"

		issues = append(issues, issue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate status mismatches: %w",
			err,
		)
	}

	return issues, nil
}

func (s *Service) findLedgerMismatches(
	ctx context.Context,
) ([]Issue, error) {
	rows, err := s.db.Query(
		ctx,
		`
		SELECT
			o.id,
			o.sku,
			o.status::text,
			d.status,
			d.request_id,
			o.paid_at,
			o.delivered_at,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'payment'
			) AS payment_ledger_entries,
			(
				SELECT count(*)
				FROM ledger_entries AS l
				WHERE l.order_id = o.id
				  AND l.entry_type = 'delivery'
			) AS delivery_ledger_entries
		FROM orders AS o
		LEFT JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE (
				o.paid_at IS NOT NULL
				AND (
					SELECT count(*)
					FROM ledger_entries AS payment
					WHERE payment.order_id = o.id
					  AND payment.entry_type = 'payment'
				) <> 1
			)
		   OR (
				o.status = 'delivered'
				AND (
					SELECT count(*)
					FROM ledger_entries AS delivered
					WHERE delivered.order_id = o.id
					  AND delivered.entry_type = 'delivery'
				) <> 1
			)
		ORDER BY o.created_at, o.id
		`,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"query ledger mismatches: %w",
			err,
		)
	}
	defer rows.Close()

	issues := make([]Issue, 0)

	for rows.Next() {
		var issue Issue

		if err := rows.Scan(
			&issue.OrderID,
			&issue.SKU,
			&issue.OrderStatus,
			&issue.DeliveryStatus,
			&issue.RequestID,
			&issue.PaidAt,
			&issue.DeliveredAt,
			&issue.PaymentLedgerEntries,
			&issue.DeliveryLedgerEntries,
		); err != nil {
			return nil, fmt.Errorf(
				"scan ledger mismatch: %w",
				err,
			)
		}

		issue.Type = IssueLedgerMismatch
		issue.Description =
			"ledger entries do not match the order state"

		issues = append(issues, issue)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate ledger mismatches: %w",
			err,
		)
	}

	return issues, nil
}

func (s *Service) RetryDelivery(
	ctx context.Context,
	orderID uuid.UUID,
) (RetryResult, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return RetryResult{}, fmt.Errorf(
			"begin retry delivery transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		orderStatus string
		paidAt      *time.Time
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			status::text,
			paid_at
		FROM orders
		WHERE id = $1
		FOR UPDATE
		`,
		orderID,
	).Scan(
		&orderStatus,
		&paidAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RetryResult{}, ErrOrderNotFound
		}

		return RetryResult{}, fmt.Errorf(
			"lock order for delivery retry: %w",
			err,
		)
	}

	if orderStatus == "delivered" {
		if err := tx.Commit(ctx); err != nil {
			return RetryResult{}, fmt.Errorf(
				"commit delivered retry request: %w",
				err,
			)
		}

		return RetryResult{
			OrderID: orderID,
			Status:  "already_delivered",
			Message: "order is already delivered",
		}, nil
	}

	if paidAt == nil ||
		orderStatus == "created" ||
		orderStatus == "payment_failed" {
		return RetryResult{}, ErrOrderNotPaid
	}

	var deliveryStatus string

	err = tx.QueryRow(
		ctx,
		`
		SELECT status
		FROM delivery_attempts
		WHERE order_id = $1
		FOR UPDATE
		`,
		orderID,
	).Scan(&deliveryStatus)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return RetryResult{}, fmt.Errorf(
				"lock delivery attempt: %w",
				err,
			)
		}

		_, err = tx.Exec(
			ctx,
			`
			INSERT INTO delivery_attempts (
				order_id,
				request_id,
				status,
				next_attempt_at
			)
			VALUES (
				$1,
				$2,
				'pending',
				now()
			)
			ON CONFLICT (order_id) DO NOTHING
			`,
			orderID,
			fmt.Sprintf("req-%s-1", orderID),
		)
		if err != nil {
			return RetryResult{}, fmt.Errorf(
				"create missing delivery attempt: %w",
				err,
			)
		}

		_, err = tx.Exec(
			ctx,
			`
			UPDATE orders
			SET
				status = 'paid',
				last_error = NULL,
				updated_at = now()
			WHERE id = $1
			`,
			orderID,
		)
		if err != nil {
			return RetryResult{}, fmt.Errorf(
				"prepare order for delivery: %w",
				err,
			)
		}

		if err := tx.Commit(ctx); err != nil {
			return RetryResult{}, fmt.Errorf(
				"commit missing delivery attempt: %w",
				err,
			)
		}

		return RetryResult{
			OrderID: orderID,
			Status:  "queued",
			Message: "missing delivery job was created",
		}, nil
	}

	switch deliveryStatus {
	case "delivered":
		if err := tx.Commit(ctx); err != nil {
			return RetryResult{}, fmt.Errorf(
				"commit delivered attempt retry: %w",
				err,
			)
		}

		return RetryResult{
			OrderID: orderID,
			Status:  "already_delivered",
			Message: "delivery attempt is already completed",
		}, nil

	case "processing":
		// Нельзя сбрасывать активный processing в pending.
		// В этот момент внешний запрос к поставщику может
		// ещё выполняться.
		return RetryResult{}, ErrDeliveryRunning

	case "pending":
		_, err = tx.Exec(
			ctx,
			`
			UPDATE delivery_attempts
			SET
				next_attempt_at = now(),
				updated_at = now()
			WHERE order_id = $1
			  AND status = 'pending'
			`,
			orderID,
		)
		if err != nil {
			return RetryResult{}, fmt.Errorf(
				"refresh pending delivery: %w",
				err,
			)
		}

	case "failed", "out_of_stock":
		_, err = tx.Exec(
			ctx,
			`
			UPDATE delivery_attempts
			SET
				status = 'pending',
				error = NULL,
				next_attempt_at = now(),
				locked_at = NULL,
				updated_at = now()
			WHERE order_id = $1
			  AND status IN (
					'failed',
					'out_of_stock'
			  )
			`,
			orderID,
		)
		if err != nil {
			return RetryResult{}, fmt.Errorf(
				"requeue delivery attempt: %w",
				err,
			)
		}

	default:
		return RetryResult{}, fmt.Errorf(
			"unsupported delivery status: %s",
			deliveryStatus,
		)
	}

	_, err = tx.Exec(
		ctx,
		`
		UPDATE orders
		SET
			status = 'paid',
			last_error = NULL,
			updated_at = now()
		WHERE id = $1
		  AND status IN (
				'paid',
				'out_of_stock',
				'delivery_failed'
		  )
		`,
		orderID,
	)
	if err != nil {
		return RetryResult{}, fmt.Errorf(
			"prepare existing delivery for retry: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return RetryResult{}, fmt.Errorf(
			"commit delivery retry: %w",
			err,
		)
	}

	return RetryResult{
		OrderID: orderID,
		Status:  "queued",
		Message: "delivery was safely queued",
	}, nil
}

func (s *Service) Repair(
	ctx context.Context,
) (RepairResult, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return RepairResult{}, fmt.Errorf(
			"begin repair transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	createdTag, err := tx.Exec(
		ctx,
		`
		INSERT INTO delivery_attempts (
			order_id,
			request_id,
			status,
			next_attempt_at
		)
		SELECT
			o.id,
			'req-' || o.id::text || '-1',
			'pending',
			now()
		FROM orders AS o
		WHERE o.paid_at IS NOT NULL
		  AND o.status IN (
				'paid',
				'out_of_stock',
				'delivery_failed'
		  )
		  AND NOT EXISTS (
				SELECT 1
				FROM delivery_attempts AS d
				WHERE d.order_id = o.id
		  )
		ON CONFLICT (order_id) DO NOTHING
		`,
	)
	if err != nil {
		return RepairResult{}, fmt.Errorf(
			"create missing delivery jobs: %w",
			err,
		)
	}

	queuedTag, err := tx.Exec(
		ctx,
		`
		UPDATE delivery_attempts AS d
		SET
			next_attempt_at = COALESCE(
				d.next_attempt_at,
				now()
			),
			updated_at = now()
		FROM orders AS o
		WHERE o.id = d.order_id
		  AND o.paid_at IS NOT NULL
		  AND o.status IN (
				'out_of_stock',
				'delivery_failed'
		  )
		  AND d.status IN (
				'out_of_stock',
				'failed'
		  )
		  AND d.next_attempt_at IS NULL
		`,
	)
	if err != nil {
		return RepairResult{}, fmt.Errorf(
			"queue recoverable delivery jobs: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return RepairResult{}, fmt.Errorf(
			"commit repair transaction: %w",
			err,
		)
	}

	return RepairResult{
		CreatedDeliveryJobs: createdTag.RowsAffected(),
		QueuedRecoverable:   queuedTag.RowsAffected(),
	}, nil
}

func (s *Service) RunRepairLoop(
	ctx context.Context,
	interval time.Duration,
) error {
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// Выполняем восстановление сразу после запуска.
	if _, err := s.Repair(ctx); err != nil {
		return fmt.Errorf(
			"initial reconciliation repair: %w",
			err,
		)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil

		case <-ticker.C:
			if _, err := s.Repair(ctx); err != nil {
				return fmt.Errorf(
					"periodic reconciliation repair: %w",
					err,
				)
			}
		}
	}
}
