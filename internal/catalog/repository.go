package catalog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DrummDaddy/digital_store/internal/model"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) GetProduct(ctx context.Context, sku string) (model.Product, error) {
	var product model.Product
	err := r.db.QueryRow(
		ctx, `SELECT 
                  sku, 
                  name, 
                  product_type,
                   price_minor,
                   currency, 
                   COALESCE(image, ''), 
                   active 
               FROM products
               WHERE SKU = $1 
               AND active = TRUE
			   `,
		sku,
	).Scan(&product.SKU,
		&product.Name,
		&product.ProductType,
		&product.PriceMinor,
		&product.Currency,
		&product.Image,
		&product.Active,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return model.Product{}, fmt.Errorf("product not found")
		}
		return model.Product{}, fmt.Errorf("error getting product: %v", err)
	}
	return product, nil
}
func (r *Repository) Seed(
	ctx context.Context,
	products []model.Product,
	keys []string,
) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin seed transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, product := range products {
		_, err := tx.Exec(
			ctx,
			`
			INSERT INTO products (
				sku,
				name,
				product_type,
				price_minor,
				currency,
				image,
				active
			)
			VALUES ($1, $2, $3, $4, $5, $6, TRUE)
			ON CONFLICT (sku) DO UPDATE SET
				name = EXCLUDED.name,
				product_type = EXCLUDED.product_type,
				price_minor = EXCLUDED.price_minor,
				currency = EXCLUDED.currency,
				image = EXCLUDED.image,
				active = TRUE
			`,
			product.SKU,
			product.Name,
			product.ProductType,
			product.PriceMinor,
			product.Currency,
			product.Image,
		)
		if err != nil {
			return fmt.Errorf("seed product %s: %w", product.SKU, err)
		}

		_, err = tx.Exec(
			ctx,
			`
			INSERT INTO inventory(sku, available)
			VALUES ($1, $2)
			ON CONFLICT (sku) DO NOTHING
			`,
			product.SKU,
			len(keys),
		)
		if err != nil {
			return fmt.Errorf("seed inventory %s: %w", product.SKU, err)
		}
	}

	_ = json.RawMessage(nil)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit seed transaction: %w", err)
	}

	return nil
}
