package catalog

type Item struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	ProductType string `json:"type"`
	Price       int64  `json:"price"`
	Currency    string `json:"currency"`
	Image       string `json:"image"`
	Available   int64  `json:"available"`
}

type Page struct {
	Items      []Item `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
	Limit      int    `json:"limit"`
}
