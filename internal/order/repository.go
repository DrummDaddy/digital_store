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
