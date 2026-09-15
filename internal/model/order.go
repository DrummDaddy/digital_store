package model

import (
	"time"

	"github.com/google/uuid"
)

type OrderStatus string

const (
	OrderCreated        OrderStatus = "created"
	OrderPaid           OrderStatus = "paid"
	OrderDelivering     OrderStatus = "delivering"
	OrderDelivered      OrderStatus = "delivered"
	OrderPaymentFailed  OrderStatus = "payment_failed"
	OrderOutOfStock     OrderStatus = "out_of_stock"
	OrderDeliveryFailed OrderStatus = "delivery_failed"
)

func (s OrderStatus) IsFinal() bool {
	return s == OrderDelivered || s == OrderPaymentFailed
}

func (s OrderStatus) IsRecoverable() bool {
	return s == OrderOutOfStock || s == OrderDeliveryFailed
}

type Product struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	ProductType string `json:"type"`
	PriceMinor  int64  `json:"price"`
	Currency    string `json:"currency"`
	Image       string `json:"image"`
	Active      bool   `json:"active"`
}

type Order struct {
	ID          uuid.UUID   `json:"id"`
	SKU         string      `json:"sku"`
	AmountMinor int64       `json:"amount"`
	Currency    string      `json:"currency"`
	Status      OrderStatus `json:"status"`

	Code        *string    `json:"code,omitempty"`
	PaidAt      *time.Time `json:"paid_at,omitempty"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
	LastError   *string    `json:"last_error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type DeliveryAttempt struct {
	ID            uuid.UUID  `json:"id"`
	OrderID       uuid.UUID  `json:"order_id"`
	RequestID     string     `json:"request_id"`
	Provider      *string    `json:"provider,omitempty"`
	Status        string     `json:"status"`
	Code          *string    `json:"code,omitempty"`
	Error         *string    `json:"error,omitempty"`
	Attempts      int        `json:"attempts"`
	NextAttemptAt *time.Time `json:"next_attempt_at,omitempty"`
}
