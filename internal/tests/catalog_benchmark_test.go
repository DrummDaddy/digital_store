package tests

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DrummDaddy/digital_store/internal/catalog"
)

func BenchmarkCatalogList(
	b *testing.B,
) {
	databaseURL := getTestDatabaseURL(b)

	ctx := context.Background()

	db, err := pgxpool.New(
		ctx,
		databaseURL,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	repository := catalog.NewRepository(db)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := repository.List(
			ctx,
			50,
			"",
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func getTestDatabaseURL(
	t testing.TB,
) string {
	t.Helper()

	value := getEnv(
		"TEST_DATABASE_URL",
		"",
	)

	if value == "" {
		t.Skip(
			"TEST_DATABASE_URL is not set",
		)
	}

	return value
}

func getEnv(
	key string,
	fallback string,
) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}

	return value
}

func valueOrFallback(
	value string,
	fallback string,
) string {
	if value == "" {
		return fallback
	}

	return value
}
