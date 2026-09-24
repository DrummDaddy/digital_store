package httpapi

type CreateOrderRequest struct {
	OrderID string `json:"order_id,omitempty"`
	SKU     string `json:"sku"`
}
