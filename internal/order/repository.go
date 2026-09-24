package order

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DrummDaddy/digital_store/internal/model"
)

var (
	ErrOrderNotFound   = errors.New("order not found")
	ErrProductNotFound = errors.New("product not found")
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{
		db: db,
	}
}

type queryRower interface {
	QueryRow(
		ctx context.Context,
		sql string,
		args ...any,
	) pgx.Row
}

func (r *Repository) Create(
	ctx context.Context,
	sku string,
) (model.Order, error) {
	return r.create(ctx, uuid.New(), sku)
}

func (r *Repository) CreateWithID(
	ctx context.Context,
	orderID uuid.UUID,
	sku string,
) (model.Order, error) {
	return r.create(ctx, orderID, sku)
}

func (r *Repository) create(
	ctx context.Context,
	orderID uuid.UUID,
	sku string,
) (model.Order, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return model.Order{}, fmt.Errorf(
			"begin create order transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		price    int64
		currency string
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			price_minor,
			currency
		FROM products
		WHERE sku = $1
		  AND active = TRUE
		`,
		sku,
	).Scan(
		&price,
		&currency,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, ErrProductNotFound
		}

		return model.Order{}, fmt.Errorf(
			"query product: %w",
			err,
		)
	}

	var result model.Order

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO orders (
			id,
			sku,
			amount_minor,
			currency,
			status
		)
		VALUES ($1, $2, $3, $4, 'created')
		RETURNING
			id,
			sku,
			amount_minor,
			currency,
			status,
			created_at,
			updated_at
		`,
		orderID,
		sku,
		price,
		currency,
	).Scan(
		&result.ID,
		&result.SKU,
		&result.AmountMinor,
		&result.Currency,
		&result.Status,
		&result.CreatedAt,
		&result.UpdatedAt,
	)
	if err != nil {
		return model.Order{}, fmt.Errorf(
			"insert order: %w",
			err,
		)
	}

	if err := applyPendingPayments(
		ctx,
		tx,
		result.ID,
		result.AmountMinor,
		result.Currency,
	); err != nil {
		return model.Order{}, fmt.Errorf(
			"apply pending payments: %w",
			err,
		)
	}

	result, err = getByIDTx(ctx, tx, result.ID)
	if err != nil {
		return model.Order{}, fmt.Errorf(
			"get created order: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return model.Order{}, fmt.Errorf(
			"commit create order transaction: %w",
			err,
		)
	}

	return result, nil
}

func (r *Repository) GetByID(
	ctx context.Context,
	id uuid.UUID,
) (model.Order, error) {
	return getByID(ctx, r.db, id)
}

func getByID(
	ctx context.Context,
	db queryRower,
	id uuid.UUID,
) (model.Order, error) {
	var result model.Order

	err := db.QueryRow(
		ctx,
		`
		SELECT
			o.id,
			o.sku,
			o.amount_minor,
			o.currency,
			o.status,
			o.paid_at,
			o.delivered_at,
			o.last_error,
			o.created_at,
			o.updated_at,
			d.code
		FROM orders AS o
		LEFT JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE o.id = $1
		`,
		id,
	).Scan(
		&result.ID,
		&result.SKU,
		&result.AmountMinor,
		&result.Currency,
		&result.Status,
		&result.PaidAt,
		&result.DeliveredAt,
		&result.LastError,
		&result.CreatedAt,
		&result.UpdatedAt,
		&result.Code,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, ErrOrderNotFound
		}

		return model.Order{}, fmt.Errorf(
			"query order: %w",
			err,
		)
	}

	return result, nil
}

func getByIDTx(
	ctx context.Context,
	tx pgx.Tx,
	id uuid.UUID,
) (model.Order, error) {
	return getByID(ctx, tx, id)
}

func (r *Repository) ProcessPayment(
	ctx context.Context,
	event model.PaymentWebhook,
	rawPayload []byte,
) error {
	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf(
			"parse order id: %w",
			err,
		)
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf(
			"begin payment transaction: %w",
			err,
		)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var insertedEventID string

	err = tx.QueryRow(
		ctx,
		`
		INSERT INTO payment_events (
			event_id,
			order_id,
			status,
			amount_minor,
			currency,
			payload
		)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb)
		ON CONFLICT (event_id) DO NOTHING
		RETURNING event_id
		`,
		event.EventID,
		orderID,
		event.Status,
		event.Amount,
		event.Currency,
		string(rawPayload),
	).Scan(&insertedEventID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {

			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf(
					"commit duplicate payment event: %w",
					err,
				)
			}

			return nil
		}

		return fmt.Errorf(
			"insert payment event: %w",
			err,
		)
	}

	var (
		currentStatus model.OrderStatus
		orderAmount   int64
		orderCurrency string
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT
			status,
			amount_minor,
			currency
		FROM orders
		WHERE id = $1
		FOR UPDATE
		`,
		orderID,
	).Scan(
		&currentStatus,
		&orderAmount,
		&orderCurrency,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {

			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf(
					"commit early payment event: %w",
					err,
				)
			}

			return nil
		}

		return fmt.Errorf(
			"lock order: %w",
			err,
		)
	}

	if err := applyPaymentToOrder(
		ctx,
		tx,
		orderID,
		currentStatus,
		orderAmount,
		orderCurrency,
		event,
	); err != nil {
		return fmt.Errorf(
			"apply payment to order: %w",
			err,
		)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf(
			"commit payment transaction: %w",
			err,
		)
	}

	return nil
}

func applyPendingPayments(
	ctx context.Context,
	tx pgx.Tx,
	orderID uuid.UUID,
	orderAmount int64,
	orderCurrency string,
) error {
	rows, err := tx.Query(
		ctx,
		`
		SELECT
			event_id,
			status,
			amount_minor,
			currency
		FROM payment_events
		WHERE order_id = $1
		  AND processed_at IS NULL
		ORDER BY received_at ASC, event_id ASC
		`,
		orderID,
	)
	if err != nil {
		return fmt.Errorf(
			"select pending payment events: %w",
			err,
		)
	}

	type pendingPayment struct {
		EventID  string
		Status   string
		Amount   int64
		Currency string
	}

	events := make([]pendingPayment, 0)

	for rows.Next() {
		var event pendingPayment

		if err := rows.Scan(
			&event.EventID,
			&event.Status,
			&event.Amount,
			&event.Currency,
		); err != nil {
			rows.Close()

			return fmt.Errorf(
				"scan pending payment event: %w",
				err,
			)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		rows.Close()

		return fmt.Errorf(
			"iterate pending payment events: %w",
			err,
		)
	}

	rows.Close()

	for _, pending := range events {
		var currentStatus model.OrderStatus

		err := tx.QueryRow(
			ctx,
			`
			SELECT status
			FROM orders
			WHERE id = $1
			FOR UPDATE
			`,
			orderID,
		).Scan(&currentStatus)
		if err != nil {
			return fmt.Errorf(
				"lock order for pending payment: %w",
				err,
			)
		}

		event := model.PaymentWebhook{
			EventID:  pending.EventID,
			OrderID:  orderID.String(),
			Status:   pending.Status,
			Amount:   pending.Amount,
			Currency: pending.Currency,
		}

		if err := applyPaymentToOrder(
			ctx,
			tx,
			orderID,
			currentStatus,
			orderAmount,
			orderCurrency,
			event,
		); err != nil {
			return fmt.Errorf(
				"apply pending event %s: %w",
				event.EventID,
				err,
			)
		}
	}

	return nil
}

func applyPaymentToOrder(
	ctx context.Context,
	tx pgx.Tx,
	orderID uuid.UUID,
	currentStatus model.OrderStatus,
	orderAmount int64,
	orderCurrency string,
	event model.PaymentWebhook,
) error {

	if event.Amount != orderAmount ||
		event.Currency != orderCurrency {
		return markPaymentProcessed(
			ctx,
			tx,
			event.EventID,
		)
	}

	switch event.Status {
	case "paid":
		return applyPaidEvent(
			ctx,
			tx,
			orderID,
			currentStatus,
			orderAmount,
			orderCurrency,
			event.EventID,
		)

	case "failed":
		return applyFailedEvent(
			ctx,
			tx,
			orderID,
			currentStatus,
			event.EventID,
		)

	default:
		return fmt.Errorf(
			"unsupported payment status: %s",
			event.Status,
		)
	}
}

func applyPaidEvent(
	ctx context.Context,
	tx pgx.Tx,
	orderID uuid.UUID,
	currentStatus model.OrderStatus,
	orderAmount int64,
	orderCurrency string,
	eventID string,
) error {
	switch currentStatus {
	case model.OrderDelivered:

		return markPaymentProcessed(ctx, tx, eventID)

	case model.OrderPaymentFailed:

		return markPaymentProcessed(ctx, tx, eventID)

	case model.OrderCreated:
		_, err := tx.Exec(
			ctx,
			`
			UPDATE orders
			SET
				status = 'paid',
				paid_at = COALESCE(paid_at, now()),
				last_error = NULL,
				updated_at = now()
			WHERE id = $1
			  AND status = 'created'
			`,
			orderID,
		)
		if err != nil {
			return fmt.Errorf(
				"mark created order paid: %w",
				err,
			)
		}

	case model.OrderOutOfStock, model.OrderDeliveryFailed:

		_, err := tx.Exec(
			ctx,
			`
			UPDATE orders
			SET
				status = 'paid',
				paid_at = COALESCE(paid_at, now()),
				last_error = NULL,
				updated_at = now()
			WHERE id = $1
			  AND status IN ('out_of_stock', 'delivery_failed')
			`,
			orderID,
		)
		if err != nil {
			return fmt.Errorf(
				"recover paid order: %w",
				err,
			)
		}

	case model.OrderPaid, model.OrderDelivering:

	}

	_, err := tx.Exec(
		ctx,
		`
		INSERT INTO delivery_attempts (
			order_id,
			request_id,
			status,
			next_attempt_at
		)
		VALUES ($1, $2, 'pending', now())
		ON CONFLICT (order_id) DO UPDATE
		SET
			status = CASE
				WHEN delivery_attempts.status IN (
					'out_of_stock',
					'failed'
				)
				THEN 'pending'
				ELSE delivery_attempts.status
			END,
			next_attempt_at = CASE
				WHEN delivery_attempts.status IN (
					'out_of_stock',
					'failed'
				)
				THEN now()
				ELSE delivery_attempts.next_attempt_at
			END,
			error = CASE
				WHEN delivery_attempts.status IN (
					'out_of_stock',
					'failed'
				)
				THEN NULL
				ELSE delivery_attempts.error
			END,
			updated_at = now()
		`,
		orderID,
		fmt.Sprintf("req-%s-1", orderID),
	)
	if err != nil {
		return fmt.Errorf(
			"create or restore delivery attempt: %w",
			err,
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
		VALUES ($1, 'payment', $2, $3)
		ON CONFLICT DO NOTHING
		`,
		orderID,
		orderAmount,
		orderCurrency,
	)
	if err != nil {
		return fmt.Errorf(
			"create payment ledger entry: %w",
			err,
		)
	}

	return markPaymentProcessed(
		ctx,
		tx,
		eventID,
	)
}

func applyFailedEvent(
	ctx context.Context,
	tx pgx.Tx,
	orderID uuid.UUID,
	currentStatus model.OrderStatus,
	eventID string,
) error {
	switch currentStatus {
	case model.OrderCreated:
		_, err := tx.Exec(
			ctx,
			`
			UPDATE orders
			SET
				status = 'payment_failed',
				updated_at = now()
			WHERE id = $1
			  AND status = 'created'
			`,
			orderID,
		)
		if err != nil {
			return fmt.Errorf(
				"mark payment failed: %w",
				err,
			)
		}

	case model.OrderPaid,
		model.OrderDelivering,
		model.OrderDelivered,
		model.OrderOutOfStock,
		model.OrderDeliveryFailed,
		model.OrderPaymentFailed:

	}

	return markPaymentProcessed(
		ctx,
		tx,
		eventID,
	)
}

func markPaymentProcessed(
	ctx context.Context,
	tx pgx.Tx,
	eventID string,
) error {
	_, err := tx.Exec(
		ctx,
		`
		UPDATE payment_events
		SET processed_at = now()
		WHERE event_id = $1
		`,
		eventID,
	)
	if err != nil {
		return fmt.Errorf(
			"mark payment event processed: %w",
			err,
		)
	}

	return nil
}
