package catalog

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) List(
	ctx context.Context,
	limit int,
	afterSKU string,
) (Page, error) {
	if limit <= 0 {
		limit = 50
	}

	if limit > 100 {
		limit = 100
	}

	query := `
		SELECT
			p.sku,
			p.name,
			p.product_type,
			p.price_minor,
			p.currency,
			COALESCE(p.image, ''),
			COALESCE(i.available, 0)
		FROM products AS p
		LEFT JOIN inventory AS i
			ON i.sku = p.sku
		WHERE p.active = TRUE
	`

	args := make([]any, 0, 2)

	if afterSKU != "" {
		args = append(args, afterSKU)
		query += fmt.Sprintf(
			" AND p.sku > $%d",
			len(args),
		)
	}

	args = append(args, limit+1)

	query += fmt.Sprintf(
		`
		ORDER BY p.sku
		LIMIT $%d
		`,
		len(args),
	)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return Page{}, fmt.Errorf(
			"list catalog: %w",
			err,
		)
	}
	defer rows.Close()

	items := make([]Item, 0, limit+1)

	for rows.Next() {
		var item Item

		if err := rows.Scan(
			&item.SKU,
			&item.Name,
			&item.ProductType,
			&item.Price,
			&item.Currency,
			&item.Image,
			&item.Available,
		); err != nil {
			return Page{}, fmt.Errorf(
				"scan catalog item: %w",
				err,
			)
		}

		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return Page{}, fmt.Errorf(
			"iterate catalog rows: %w",
			err,
		)
	}

	page := Page{
		Items: items,
		Limit: limit,
	}

	if len(items) > limit {
		page.Items = items[:limit]
		page.NextCursor = page.Items[len(page.Items)-1].SKU
	}

	return page, nil
}
