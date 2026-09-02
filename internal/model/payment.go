package model

import "time"

type PaymentWebhook struct {
	EventID   string    `json:"event_id"`
	OrderID   string    `json:"order_id"`
	Status    string    `json:"status"`
	Amount    int64     `json:"amount"`
	Currency  string    `json:"currency"`
	CreatedAt time.Time `json:"created_at"`
}
