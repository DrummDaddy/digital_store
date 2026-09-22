package delivery

import (
	"context"
	"errors"
)

var (
	ErrProviderUnavailable = errors.New("provider unavailable")
	ErrProviderTimeout     = errors.New("provider timeout")
	ErrOutOfStock          = errors.New("out of stock")
	ErrInvalidResponse     = errors.New("invalid provider response")
)

type IssueRequest struct {
	RequestID string `json:"request_id"`
	SKU       string `json:"sku"`
	OrderID   string `json:"order_id"`
}

type IssueResponse struct {
	Status    string `json:"status"`
	RequestID string `json:"request_id,omitempty"`
	Code      string `json:"code,omitempty"`
	Reason    string `json:"reason,omitempty"`
}

type Provider interface {
	Name() string

	Issue(
		ctx context.Context,
		request IssueRequest,
	) (IssueResponse, error)
}
