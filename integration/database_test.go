package integration

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"cartrecovery/commons"
	"github.com/jackc/pgx/v5"
)

// Each test owns a schema so the verifier cannot erase demo cart data.
func testDatabaseURL(t *testing.T) string {
	t.Helper()
	baseURL := setting("DATABASE_URL", "postgres://cart:cart@localhost:15432/cart_recovery?sslmode=disable")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := commons.OpenPostgres(ctx, baseURL)
	if err != nil {
		t.Fatalf("start PostgreSQL before running the verifier: %v", err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("recovery_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	t.Logf("PostgreSQL schema: %s", schema)
	if _, err = admin.Pool.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		admin, err := commons.OpenPostgres(ctx, baseURL)
		if err != nil {
			t.Error(err)
			return
		}
		defer admin.Close()
		if _, err = admin.Pool.Exec(ctx, "DROP SCHEMA "+identifier+" CASCADE"); err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(baseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	return u.String()
}
