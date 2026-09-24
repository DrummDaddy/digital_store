package reconciliation

import (
	"time"

	"github.com/google/uuid"
)

type IssueType string

const (
	IssuePaidNotDelivered IssueType = "paid_not_delivered"
	IssueDeliveredNotPaid IssueType = "delivered_not_paid"
	IssueOrderMismatch    IssueType = "order_status_mismatch"
	IssueLedgerMismatch   IssueType = "ledger_mismatch"
)

type Issue struct {
	Type IssueType `json:"type"`

	OrderID uuid.UUID `json:"order_id"`
	SKU     string    `json:"sku"`

	OrderStatus    string  `json:"order_status"`
	DeliveryStatus *string `json:"delivery_status,omitempty"`
	RequestID      *string `json:"request_id,omitempty"`

	PaidAt      *time.Time `json:"paid_at,omitempty"`
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`

	PaymentLedgerEntries  int64 `json:"payment_ledger_entries"`
	DeliveryLedgerEntries int64 `json:"delivery_ledger_entries"`

	Description string `json:"description"`
}

type Report struct {
	GeneratedAt time.Time `json:"generated_at"`

	PaidNotDelivered []Issue `json:"paid_not_delivered"`
	DeliveredNotPaid []Issue `json:"delivered_not_paid"`
	StatusMismatches []Issue `json:"status_mismatches"`
	LedgerMismatches []Issue `json:"ledger_mismatches"`

	Summary Summary `json:"summary"`
}

type Summary struct {
	PaidNotDelivered int `json:"paid_not_delivered"`
	DeliveredNotPaid int `json:"delivered_not_paid"`
	StatusMismatches int `json:"status_mismatches"`
	LedgerMismatches int `json:"ledger_mismatches"`
	Total            int `json:"total"`
}

type RepairResult struct {
	CreatedDeliveryJobs int64 `json:"created_delivery_jobs"`
	QueuedRecoverable   int64 `json:"queued_recoverable"`
}

type RetryResult struct {
	OrderID uuid.UUID `json:"order_id"`
	Status  string    `json:"status"`
	Message string    `json:"message"`
}
