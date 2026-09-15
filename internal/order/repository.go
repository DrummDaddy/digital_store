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
	return &Repository{db: db}
}

func (r *Repository) Create(ctx context.Context, sku string) (model.Order, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return model.Order{}, fmt.Errorf("error starting transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	var (
		price    int64
		currency string
	)

	err = tx.QueryRow(ctx,
		`SELECT price_minor, currency
             FROM products 
             WHERE sku = $1
             AND active = TRUE`, sku).Scan(&price, &currency)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, ErrProductNotFound
		}
		return model.Order{}, fmt.Errorf("error querying products: %w", err)
	}

	var order model.Order

	err = tx.QueryRow(ctx,
		`INSERT INTO orders (
             sku, 
             amoount_minor, 
             currency, 
             status
			)
          VALUES ($1, $2, $3, 'created'
          RETURNING 
          id, 
          sku, 
          amount_minor,
                  currency,
                  status, 
                  created_at,
                  updated_at
                  `, sku,
		price,
		currency).Scan(&order.ID,
		&order.SKU,
		&order.AmountMinor,
		&order.Currency,
		&order.Status,
		&order.CreatedAt,
		&order.UpdatedAt)
	if err != nil {
		return model.Order{}, fmt.Errorf("error inserting order: %w", err)
	}

	err = applyPendingPayments(ctx, tx, order.ID, order.AmountMinor, order.Currency)
	if err != nil {
		return model.Order{}, fmt.Errorf("error applying pending payments: %w", err)
	}
	order, err = getByIDTx(ctx, tx, order.ID)
	if err != nil {
		return model.Order{}, fmt.Errorf("error getting order: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return model.Order{}, fmt.Errorf("error committing order: %w", err)
	}
	return order, nil
}

func (r *Repository) GetByID(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id uuid.UUID) (model.Order, error) {

	var (
		order model.Order
		code  *string
	)

	err := db.QueryRow(
		ctx, `
        SELECT 
          o.id, 
           o.sku, 
           o.amount_minor, 
           o.currency, 
           o.status 
           o.paid_at, 
           o.delivered_at
           o.last_error, 
           o.created_at, 
           o.updated_at
           d.code 
     FROM orders o
    LEFT JOIN delivery_attempts d 
        ON d.order_id = o.id
        WHERE o.id = $1
 `, id).Scan(&order.ID,
		&order.SKU,
		&order.AmountMinor,
		&order.Currency,
		&order.Status,
		&order.PaidAt,
		&order.DeliveredAt,
		&order.LastError,
		&order.CreatedAt,
		&order.UpdatedAt,
		&code)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Order{}, ErrOrderNotFound

		}
		return model.Order{}, fmt.Errorf("error querying order: %w", err)
	}
	order.Code = code
	return order, nil
}

func getByIDTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (model.Order, error) {
	return getByIDTx(ctx, tx, id)
}

func (r *Repository) ProcessPayment(
	ctx context.Context,
	event model.PaymentWebhook,
	rawPayload []byte,
) error {
	orderID, err := uuid.Parse(event.OrderID)
	if err != nil {
		return fmt.Errorf("parse order id: %w", err)
	}

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin payment transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Сначала регистрируем событие.
	// Если такой event_id уже есть, это безопасный повтор.
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
				return fmt.Errorf("commit duplicate payment event: %w", err)
			}

			return nil
		}

		return fmt.Errorf("insert payment event: %w", err)
	}

	var (
		orderStatus model.OrderStatus
		orderAmount int64
		orderCurr   string
	)

	err = tx.QueryRow(
		ctx,
		`
		SELECT status, amount_minor, currency
		FROM orders
		WHERE id = $1
		FOR UPDATE
		`,
		orderID,
	).Scan(
		&orderStatus,
		&orderAmount,
		&orderCurr,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit early payment event: %w", err)
			}

			return nil
		}

		return fmt.Errorf("lock order: %w", err)
	}

	if err := applyPaymentToOrder(
		ctx,
		tx,
		orderID,
		orderStatus,
		orderAmount,
		orderCurr,
		event,
	); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit payment transaction: %w", err)
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
		ORDER BY received_at ASC
		`,
		orderID,
	)
	if err != nil {
		return fmt.Errorf("select pending payment events: %w", err)
	}
	defer rows.Close()

	type pendingPayment struct {
		EventID  string
		Status   string
		Amount   int64
		Currency string
	}

	var events []pendingPayment

	for rows.Next() {
		var event pendingPayment

		if err := rows.Scan(
			&event.EventID,
			&event.Status,
			&event.Amount,
			&event.Currency,
		); err != nil {
			return fmt.Errorf("scan pending payment event: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate pending payment events: %w", err)
	}

	for _, pending := range events {
		event := model.PaymentWebhook{
			EventID:  pending.EventID,
			Status:   pending.Status,
			Amount:   pending.Amount,
			Currency: pending.Currency,
		}

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
			return fmt.Errorf("lock order for pending payment: %w", err)
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
			return err
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

	if event.Amount != orderAmount || event.Currency != orderCurrency {
		_, err := tx.Exec(
			ctx,
			`
			UPDATE payment_events
			SET processed_at = now()
			WHERE event_id = $1
			`,
			event.EventID,
		)
		if err != nil {
			return fmt.Errorf("mark invalid payment event processed: %w", err)
		}

		return fmt.Errorf(
			"payment amount or currency mismatch: event=%s",
			event.EventID,
		)
	}

	switch event.Status {
	case "paid":
		if currentStatus == model.OrderDelivered ||
			currentStatus == model.OrderPaymentFailed {

			return markPaymentProcessed(ctx, tx, event.EventID)
		}

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
			`,
			orderID,
		)
		if err != nil {
			return fmt.Errorf("mark order paid: %w", err)
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
			VALUES ($1, $2, 'pending', now())
			ON CONFLICT (order_id) DO NOTHING
			`,
			orderID,
			fmt.Sprintf("req-%s-1", orderID.String()),
		)
		if err != nil {
			return fmt.Errorf("create delivery attempt: %w", err)
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
			return fmt.Errorf("create payment ledger entry: %w", err)
		}

	case "failed":
		if currentStatus == model.OrderDelivered {
			return markPaymentProcessed(ctx, tx, event.EventID)
		}

		if currentStatus == model.OrderPaid ||
			currentStatus == model.OrderDelivering ||
			currentStatus == model.OrderOutOfStock ||
			currentStatus == model.OrderDeliveryFailed {

			return markPaymentProcessed(ctx, tx, event.EventID)
		}

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
			return fmt.Errorf("mark payment failed: %w", err)
		}

	default:
		return fmt.Errorf("unsupported payment status: %s", event.Status)
	}

	return markPaymentProcessed(ctx, tx, event.EventID)
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
		return fmt.Errorf("mark payment event processed: %w", err)
	}

	return nil
}
