package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DrummDaddy/digital_store/internal/reconciliation"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DrummDaddy/digital_store/internal/delivery"
	"github.com/DrummDaddy/digital_store/internal/httpapi"
	"github.com/DrummDaddy/digital_store/internal/model"
	"github.com/DrummDaddy/digital_store/internal/order"
)

const testSKU = "STEAM-TOPUP-500"

func TestFiftyParallelPaymentWebhooksExactlyOnce(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()
	orderRepository := order.NewRepository(db)

	apiServer := httptest.NewServer(
		httpapi.NewServer(
			orderRepository,
			logger,
		).Router(),
	)
	t.Cleanup(apiServer.Close)

	createdOrder := createOrder(
		t,
		apiServer.URL,
		"",
		testSKU,
	)

	const webhookCount = 50

	start := make(chan struct{})
	errorsChannel := make(
		chan error,
		webhookCount,
	)

	var waitGroup sync.WaitGroup

	for i := 0; i < webhookCount; i++ {
		waitGroup.Add(1)

		go func(index int) {
			defer waitGroup.Done()

			<-start

			event := model.PaymentWebhook{
				EventID: fmt.Sprintf(
					"evt-race-%03d",
					index,
				),
				OrderID:   createdOrder.ID.String(),
				Status:    "paid",
				Amount:    500,
				Currency:  "RUB",
				CreatedAt: time.Now().UTC(),
			}

			if err := postWebhook(
				apiServer.URL,
				event,
			); err != nil {
				errorsChannel <- err
			}
		}(i)
	}

	close(start)

	waitGroup.Wait()
	close(errorsChannel)

	for err := range errorsChannel {
		t.Errorf(
			"parallel webhook failed: %v",
			err,
		)
	}

	if t.Failed() {
		t.FailNow()
	}

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM payment_events
		WHERE order_id = $1
		`,
		webhookCount,
		createdOrder.ID,
	)

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		1,
		createdOrder.ID,
	)

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM ledger_entries
		WHERE order_id = $1
		  AND entry_type = 'payment'
		`,
		1,
		createdOrder.ID,
	)

	var providerCalls atomic.Int64

	providerServer := newSuccessfulProvider(
		t,
		"RACE-TEST-CODE",
		&providerCalls,
	)

	deliveryRepository := delivery.NewRepository(db)

	providerA := delivery.NewHTTPProvider(
		"provider_a",
		providerServer.URL,
		time.Second,
	)

	providerB := delivery.NewHTTPProvider(
		"provider_b",
		providerServer.URL,
		time.Second,
	)

	deliveryService := delivery.NewService(
		deliveryRepository,
		providerA,
		providerB,
		3,
		time.Second,
		logger,
	)

	job, found, err := deliveryRepository.ClaimNextJob(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"claim delivery job: %v",
			err,
		)
	}

	if !found {
		t.Fatal("delivery job was not found")
	}

	if err := deliveryService.Process(
		context.Background(),
		job,
	); err != nil {
		t.Fatalf(
			"process delivery job: %v",
			err,
		)
	}

	if providerCalls.Load() != 1 {
		t.Fatalf(
			"expected one provider call, got %d",
			providerCalls.Load(),
		)
	}

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM delivery_attempts
		WHERE order_id = $1
		  AND status = 'delivered'
		`,
		1,
		createdOrder.ID,
	)

	var (
		orderStatus   string
		deliveredCode string
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			o.status,
			d.code
		FROM orders AS o
		JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE o.id = $1
		`,
		createdOrder.ID,
	).Scan(
		&orderStatus,
		&deliveredCode,
	)
	if err != nil {
		t.Fatalf(
			"query delivered order: %v",
			err,
		)
	}

	if orderStatus != "delivered" {
		t.Fatalf(
			"expected order status delivered, got %s",
			orderStatus,
		)
	}

	if deliveredCode != "RACE-TEST-CODE" {
		t.Fatalf(
			"unexpected delivered code: %s",
			deliveredCode,
		)
	}
}

func TestDuplicateEventIDIsIdempotent(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()
	orderRepository := order.NewRepository(db)

	apiServer := httptest.NewServer(
		httpapi.NewServer(
			orderRepository,
			logger,
		).Router(),
	)
	t.Cleanup(apiServer.Close)

	createdOrder := createOrder(
		t,
		apiServer.URL,
		"",
		testSKU,
	)

	event := model.PaymentWebhook{
		EventID:   "evt-duplicate-001",
		OrderID:   createdOrder.ID.String(),
		Status:    "paid",
		Amount:    500,
		Currency:  "RUB",
		CreatedAt: time.Now().UTC(),
	}

	const requests = 20

	var waitGroup sync.WaitGroup
	errorsChannel := make(
		chan error,
		requests,
	)

	for i := 0; i < requests; i++ {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			if err := postWebhook(
				apiServer.URL,
				event,
			); err != nil {
				errorsChannel <- err
			}
		}()
	}

	waitGroup.Wait()
	close(errorsChannel)

	for err := range errorsChannel {
		t.Errorf(
			"duplicate webhook failed: %v",
			err,
		)
	}

	if t.Failed() {
		t.FailNow()
	}

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM payment_events
		WHERE event_id = $1
		`,
		1,
		event.EventID,
	)

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		1,
		createdOrder.ID,
	)

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM ledger_entries
		WHERE order_id = $1
		  AND entry_type = 'payment'
		`,
		1,
		createdOrder.ID,
	)
}

func TestPaymentWebhookBeforeOrder(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()
	orderRepository := order.NewRepository(db)

	apiServer := httptest.NewServer(
		httpapi.NewServer(
			orderRepository,
			logger,
		).Router(),
	)
	t.Cleanup(apiServer.Close)

	orderID := uuid.New()

	event := model.PaymentWebhook{
		EventID:   "evt-before-order-001",
		OrderID:   orderID.String(),
		Status:    "paid",
		Amount:    500,
		Currency:  "RUB",
		CreatedAt: time.Now().UTC(),
	}

	if err := postWebhook(
		apiServer.URL,
		event,
	); err != nil {
		t.Fatalf(
			"post early webhook: %v",
			err,
		)
	}

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM payment_events
		WHERE event_id = $1
		  AND processed_at IS NULL
		`,
		1,
		event.EventID,
	)

	createdOrder := createOrder(
		t,
		apiServer.URL,
		orderID.String(),
		testSKU,
	)

	if createdOrder.ID != orderID {
		t.Fatalf(
			"expected order id %s, got %s",
			orderID,
			createdOrder.ID,
		)
	}

	if createdOrder.Status != model.OrderPaid {
		t.Fatalf(
			"expected paid status, got %s",
			createdOrder.Status,
		)
	}

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM payment_events
		WHERE event_id = $1
		  AND processed_at IS NOT NULL
		`,
		1,
		event.EventID,
	)

	assertCount(
		t,
		db,
		`
		SELECT count(*)
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		1,
		orderID,
	)
}

func TestTimeoutAfterIssueReturnsSameCode(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()

	createdOrder := createPaidOrder(
		t,
		db,
		"evt-timeout-001",
	)

	var (
		mutex        sync.Mutex
		issues       = make(map[string]string)
		requestCalls int
		actualIssues int
	)

	providerServer := httptest.NewServer(
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				var request delivery.IssueRequest

				if err := json.NewDecoder(r.Body).
					Decode(&request); err != nil {
					http.Error(
						w,
						"invalid request",
						http.StatusBadRequest,
					)
					return
				}

				mutex.Lock()

				requestCalls++

				code, exists := issues[request.RequestID]
				if !exists {
					code = "TIMEOUT-SAME-CODE"
					issues[request.RequestID] = code
					actualIssues++
				}

				mutex.Unlock()

				// Первый запрос фактически выдаёт код,
				// но отвечает позже клиентского timeout.
				if !exists {
					time.Sleep(250 * time.Millisecond)
				}

				writeProviderResponse(
					w,
					http.StatusOK,
					delivery.IssueResponse{
						Status:    "ok",
						RequestID: request.RequestID,
						Code:      code,
					},
				)
			},
		),
	)
	t.Cleanup(providerServer.Close)

	deliveryRepository := delivery.NewRepository(db)

	providerA := delivery.NewHTTPProvider(
		"provider_a",
		providerServer.URL,
		50*time.Millisecond,
	)

	providerB := delivery.NewHTTPProvider(
		"provider_b",
		providerServer.URL,
		50*time.Millisecond,
	)

	deliveryService := delivery.NewService(
		deliveryRepository,
		providerA,
		providerB,
		3,
		time.Second,
		logger,
	)

	job, found, err := deliveryRepository.ClaimNextJob(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"claim delivery job: %v",
			err,
		)
	}

	if !found {
		t.Fatal("delivery job not found")
	}

	if err := deliveryService.Process(
		context.Background(),
		job,
	); err != nil {
		t.Fatalf(
			"process timeout delivery: %v",
			err,
		)
	}

	mutex.Lock()
	finalRequestCalls := requestCalls
	finalActualIssues := actualIssues
	issuedCode := issues[job.RequestID]
	mutex.Unlock()

	if finalRequestCalls < 2 {
		t.Fatalf(
			"expected retry after timeout, calls=%d",
			finalRequestCalls,
		)
	}

	if finalActualIssues != 1 {
		t.Fatalf(
			"expected exactly one actual issue, got %d",
			finalActualIssues,
		)
	}

	if issuedCode != "TIMEOUT-SAME-CODE" {
		t.Fatalf(
			"unexpected provider code: %s",
			issuedCode,
		)
	}

	var (
		status string
		code   string
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			status,
			code
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		createdOrder.ID,
	).Scan(
		&status,
		&code,
	)
	if err != nil {
		t.Fatalf(
			"query delivery result: %v",
			err,
		)
	}

	if status != "delivered" {
		t.Fatalf(
			"expected delivered, got %s",
			status,
		)
	}

	if code != "TIMEOUT-SAME-CODE" {
		t.Fatalf(
			"expected same code, got %s",
			code,
		)
	}
}

func TestFallbackFromProviderAToProviderB(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()

	createdOrder := createPaidOrder(
		t,
		db,
		"evt-fallback-001",
	)

	var providerACalls atomic.Int64
	var providerBCalls atomic.Int64

	providerAServer := httptest.NewServer(
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				providerACalls.Add(1)

				writeProviderResponse(
					w,
					http.StatusServiceUnavailable,
					delivery.IssueResponse{
						Status: "error",
						Reason: "provider_unavailable",
					},
				)
			},
		),
	)
	t.Cleanup(providerAServer.Close)

	providerBServer := httptest.NewServer(
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				providerBCalls.Add(1)

				var request delivery.IssueRequest

				if err := json.NewDecoder(r.Body).
					Decode(&request); err != nil {
					http.Error(
						w,
						"invalid request",
						http.StatusBadRequest,
					)
					return
				}

				writeProviderResponse(
					w,
					http.StatusOK,
					delivery.IssueResponse{
						Status:    "ok",
						RequestID: request.RequestID,
						Code:      "FALLBACK-B-CODE",
					},
				)
			},
		),
	)
	t.Cleanup(providerBServer.Close)

	deliveryRepository := delivery.NewRepository(db)

	providerA := delivery.NewHTTPProvider(
		"provider_a",
		providerAServer.URL,
		time.Second,
	)

	providerB := delivery.NewHTTPProvider(
		"provider_b",
		providerBServer.URL,
		time.Second,
	)

	deliveryService := delivery.NewService(
		deliveryRepository,
		providerA,
		providerB,
		3,
		time.Second,
		logger,
	)

	job, found, err := deliveryRepository.ClaimNextJob(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"claim delivery job: %v",
			err,
		)
	}

	if !found {
		t.Fatal("delivery job not found")
	}

	if err := deliveryService.Process(
		context.Background(),
		job,
	); err != nil {
		t.Fatalf(
			"process fallback delivery: %v",
			err,
		)
	}

	if providerACalls.Load() != 3 {
		t.Fatalf(
			"expected 3 provider A attempts, got %d",
			providerACalls.Load(),
		)
	}

	if providerBCalls.Load() != 1 {
		t.Fatalf(
			"expected one provider B call, got %d",
			providerBCalls.Load(),
		)
	}

	var (
		status   string
		provider string
		code     string
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			status,
			provider,
			code
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		createdOrder.ID,
	).Scan(
		&status,
		&provider,
		&code,
	)
	if err != nil {
		t.Fatalf(
			"query fallback delivery: %v",
			err,
		)
	}

	if status != "delivered" {
		t.Fatalf(
			"expected delivered, got %s",
			status,
		)
	}

	if provider != "provider_b" {
		t.Fatalf(
			"expected provider_b, got %s",
			provider,
		)
	}

	if code != "FALLBACK-B-CODE" {
		t.Fatalf(
			"unexpected fallback code: %s",
			code,
		)
	}
}

func TestOutOfStockIsRecoverable(
	t *testing.T,
) {
	db := prepareDatabase(t)

	logger := testLogger()

	createdOrder := createPaidOrder(
		t,
		db,
		"evt-out-of-stock-001",
	)

	outOfStockServer := httptest.NewServer(
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				writeProviderResponse(
					w,
					http.StatusConflict,
					delivery.IssueResponse{
						Status: "error",
						Reason: "out_of_stock",
					},
				)
			},
		),
	)
	t.Cleanup(outOfStockServer.Close)

	deliveryRepository := delivery.NewRepository(db)

	providerA := delivery.NewHTTPProvider(
		"provider_a",
		outOfStockServer.URL,
		time.Second,
	)

	providerB := delivery.NewHTTPProvider(
		"provider_b",
		outOfStockServer.URL,
		time.Second,
	)

	deliveryService := delivery.NewService(
		deliveryRepository,
		providerA,
		providerB,
		3,
		time.Minute,
		logger,
	)

	job, found, err := deliveryRepository.ClaimNextJob(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"claim delivery job: %v",
			err,
		)
	}

	if !found {
		t.Fatal("delivery job not found")
	}

	if err := deliveryService.Process(
		context.Background(),
		job,
	); err != nil {
		t.Fatalf(
			"process out-of-stock delivery: %v",
			err,
		)
	}

	var (
		orderStatus    string
		deliveryStatus string
		nextAttemptAt  *time.Time
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			o.status,
			d.status,
			d.next_attempt_at
		FROM orders AS o
		JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE o.id = $1
		`,
		createdOrder.ID,
	).Scan(
		&orderStatus,
		&deliveryStatus,
		&nextAttemptAt,
	)
	if err != nil {
		t.Fatalf(
			"query out-of-stock state: %v",
			err,
		)
	}

	if orderStatus != "out_of_stock" {
		t.Fatalf(
			"expected order out_of_stock, got %s",
			orderStatus,
		)
	}

	if deliveryStatus != "out_of_stock" {
		t.Fatalf(
			"expected delivery out_of_stock, got %s",
			deliveryStatus,
		)
	}

	if nextAttemptAt == nil {
		t.Fatal(
			"expected next_attempt_at for recovery",
		)
	}
}

func prepareDatabase(
	t *testing.T,
) *pgxpool.Pool {
	t.Helper()

	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip(
			"TEST_DATABASE_URL is not set; integration test skipped",
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		20*time.Second,
	)
	defer cancel()

	db, err := pgxpool.New(
		ctx,
		databaseURL,
	)
	if err != nil {
		t.Fatalf(
			"connect test database: %v",
			err,
		)
	}

	t.Cleanup(db.Close)

	if err := db.Ping(ctx); err != nil {
		t.Fatalf(
			"ping test database: %v",
			err,
		)
	}

	applyMigrations(t, db)

	_, err = db.Exec(
		ctx,
		`
		TRUNCATE TABLE
			ledger_entries,
			provider_issues,
			provider_keys,
			delivery_attempts,
			payment_events,
			orders,
			inventory,
			products
		RESTART IDENTITY CASCADE
		`,
	)
	if err != nil {
		t.Fatalf(
			"truncate test database: %v",
			err,
		)
	}

	_, err = db.Exec(
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
		VALUES (
			$1,
			'Пополнение Steam 500 ₽',
			'topup',
			500,
			'RUB',
			'assets/steam.png',
			TRUE
		)
		`,
		testSKU,
	)
	if err != nil {
		t.Fatalf(
			"seed test product: %v",
			err,
		)
	}

	_, err = db.Exec(
		ctx,
		`
		INSERT INTO inventory (
			sku,
			available,
			reserved
		)
		VALUES ($1, 100, 0)
		`,
		testSKU,
	)
	if err != nil {
		t.Fatalf(
			"seed test inventory: %v",
			err,
		)
	}

	return db
}

func applyMigrations(
	t *testing.T,
	db *pgxpool.Pool,
) {
	t.Helper()

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate integration test file")
	}

	projectRoot := filepath.Clean(
		filepath.Join(
			filepath.Dir(currentFile),
			"..",
		),
	)

	migrations := []string{
		"001_init.sql",
		"002_delivery_worker.sql",
		"003_delivery_ledger_unique.sql",
		"004_reconciliation.sql",
	}

	for _, migrationName := range migrations {
		migrationPath := filepath.Join(
			projectRoot,
			"migrations",
			migrationName,
		)

		content, err := os.ReadFile(migrationPath)
		if err != nil {
			t.Fatalf(
				"read migration %s: %v",
				migrationName,
				err,
			)
		}

		if _, err := db.Exec(
			context.Background(),
			string(content),
		); err != nil {
			t.Fatalf(
				"apply migration %s: %v",
				migrationName,
				err,
			)
		}
	}
}

func createOrder(
	t *testing.T,
	baseURL string,
	orderID string,
	sku string,
) model.Order {
	t.Helper()

	payload := map[string]string{
		"sku": sku,
	}

	if orderID != "" {
		payload["order_id"] = orderID
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf(
			"marshal create order request: %v",
			err,
		)
	}

	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/api/orders",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf(
			"create order HTTP request: %v",
			err,
		)
	}

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf(
			"send create order request: %v",
			err,
		)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf(
			"read create order response: %v",
			err,
		)
	}

	if response.StatusCode != http.StatusCreated {
		t.Fatalf(
			"unexpected create order status %d: %s",
			response.StatusCode,
			string(responseBody),
		)
	}

	var result model.Order

	if err := json.Unmarshal(
		responseBody,
		&result,
	); err != nil {
		t.Fatalf(
			"decode create order response: %v",
			err,
		)
	}

	return result
}

func createPaidOrder(
	t *testing.T,
	db *pgxpool.Pool,
	eventID string,
) model.Order {
	t.Helper()

	orderRepository := order.NewRepository(db)

	createdOrder, err := orderRepository.Create(
		context.Background(),
		testSKU,
	)
	if err != nil {
		t.Fatalf(
			"create paid test order: %v",
			err,
		)
	}

	event := model.PaymentWebhook{
		EventID:   eventID,
		OrderID:   createdOrder.ID.String(),
		Status:    "paid",
		Amount:    500,
		Currency:  "RUB",
		CreatedAt: time.Now().UTC(),
	}

	rawPayload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf(
			"marshal payment event: %v",
			err,
		)
	}

	if err := orderRepository.ProcessPayment(
		context.Background(),
		event,
		rawPayload,
	); err != nil {
		t.Fatalf(
			"process payment event: %v",
			err,
		)
	}

	result, err := orderRepository.GetByID(
		context.Background(),
		createdOrder.ID,
	)
	if err != nil {
		t.Fatalf(
			"get paid order: %v",
			err,
		)
	}

	if result.Status != model.OrderPaid {
		t.Fatalf(
			"expected paid order, got %s",
			result.Status,
		)
	}

	return result
}

func postWebhook(
	baseURL string,
	event model.PaymentWebhook,
) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf(
			"marshal webhook: %w",
			err,
		)
	}

	request, err := http.NewRequest(
		http.MethodPost,
		baseURL+"/api/webhooks/payment",
		bytes.NewReader(body),
	)
	if err != nil {
		return fmt.Errorf(
			"create webhook request: %w",
			err,
		)
	}

	request.Header.Set(
		"Content-Type",
		"application/json",
	)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf(
			"send webhook: %w",
			err,
		)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf(
			"read webhook response: %w",
			err,
		)
	}

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"unexpected webhook status %d: %s",
			response.StatusCode,
			string(responseBody),
		)
	}

	return nil
}

func newSuccessfulProvider(
	t *testing.T,
	code string,
	calls *atomic.Int64,
) *httptest.Server {
	t.Helper()

	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				w http.ResponseWriter,
				r *http.Request,
			) {
				calls.Add(1)

				var request delivery.IssueRequest

				if err := json.NewDecoder(r.Body).
					Decode(&request); err != nil {
					http.Error(
						w,
						"invalid request",
						http.StatusBadRequest,
					)
					return
				}

				writeProviderResponse(
					w,
					http.StatusOK,
					delivery.IssueResponse{
						Status:    "ok",
						RequestID: request.RequestID,
						Code:      code,
					},
				)
			},
		),
	)

	t.Cleanup(server.Close)

	return server
}

func writeProviderResponse(
	w http.ResponseWriter,
	status int,
	response delivery.IssueResponse,
) {
	w.Header().Set(
		"Content-Type",
		"application/json",
	)
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(response)
}

func assertCount(
	t *testing.T,
	db *pgxpool.Pool,
	query string,
	expected int,
	arguments ...any,
) {
	t.Helper()

	var actual int

	if err := db.QueryRow(
		context.Background(),
		query,
		arguments...,
	).Scan(&actual); err != nil {
		t.Fatalf(
			"query count: %v",
			err,
		)
	}

	if actual != expected {
		t.Fatalf(
			"expected count %d, got %d",
			expected,
			actual,
		)
	}
}

func testLogger() *slog.Logger {
	return slog.New(
		slog.NewTextHandler(
			io.Discard,
			nil,
		),
	)
}

func TestRetryRecoverableDelivery(
	t *testing.T,
) {
	db := prepareDatabase(t)

	createdOrder := createPaidOrder(
		t,
		db,
		"evt-retry-recoverable-001",
	)

	_, err := db.Exec(
		context.Background(),
		`
		UPDATE orders
		SET
			status = 'delivery_failed',
			last_error = 'test failure',
			updated_at = now()
		WHERE id = $1
		`,
		createdOrder.ID,
	)
	if err != nil {
		t.Fatalf(
			"mark test order delivery_failed: %v",
			err,
		)
	}

	_, err = db.Exec(
		context.Background(),
		`
		UPDATE delivery_attempts
		SET
			status = 'failed',
			error = 'test failure',
			next_attempt_at = now() + interval '1 hour',
			updated_at = now()
		WHERE order_id = $1
		`,
		createdOrder.ID,
	)
	if err != nil {
		t.Fatalf(
			"mark test delivery failed: %v",
			err,
		)
	}

	service := reconciliation.NewService(db)

	result, err := service.RetryDelivery(
		context.Background(),
		createdOrder.ID,
	)
	if err != nil {
		t.Fatalf(
			"retry recoverable delivery: %v",
			err,
		)
	}

	if result.Status != "queued" {
		t.Fatalf(
			"expected queued, got %s",
			result.Status,
		)
	}

	var (
		orderStatus    string
		deliveryStatus string
		requestID      string
		nextAttemptAt  time.Time
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			o.status::text,
			d.status,
			d.request_id,
			d.next_attempt_at
		FROM orders AS o
		JOIN delivery_attempts AS d
			ON d.order_id = o.id
		WHERE o.id = $1
		`,
		createdOrder.ID,
	).Scan(
		&orderStatus,
		&deliveryStatus,
		&requestID,
		&nextAttemptAt,
	)
	if err != nil {
		t.Fatalf(
			"query retried delivery: %v",
			err,
		)
	}

	if orderStatus != "paid" {
		t.Fatalf(
			"expected order paid, got %s",
			orderStatus,
		)
	}

	if deliveryStatus != "pending" {
		t.Fatalf(
			"expected delivery pending, got %s",
			deliveryStatus,
		)
	}

	expectedRequestID := fmt.Sprintf(
		"req-%s-1",
		createdOrder.ID,
	)

	if requestID != expectedRequestID {
		t.Fatalf(
			"request_id changed: expected %s, got %s",
			expectedRequestID,
			requestID,
		)
	}

	if nextAttemptAt.After(
		time.Now().Add(5 * time.Second),
	) {
		t.Fatalf(
			"delivery was not scheduled immediately: %s",
			nextAttemptAt,
		)
	}
}

func TestRepairCreatesMissingDeliveryJob(
	t *testing.T,
) {
	db := prepareDatabase(t)

	createdOrder := createPaidOrder(
		t,
		db,
		"evt-repair-missing-job-001",
	)

	_, err := db.Exec(
		context.Background(),
		`
		DELETE FROM delivery_attempts
		WHERE order_id = $1
		`,
		createdOrder.ID,
	)
	if err != nil {
		t.Fatalf(
			"delete delivery job: %v",
			err,
		)
	}

	service := reconciliation.NewService(db)

	result, err := service.Repair(
		context.Background(),
	)
	if err != nil {
		t.Fatalf(
			"repair missing delivery job: %v",
			err,
		)
	}

	if result.CreatedDeliveryJobs != 1 {
		t.Fatalf(
			"expected one created job, got %d",
			result.CreatedDeliveryJobs,
		)
	}

	var (
		requestID string
		status    string
	)

	err = db.QueryRow(
		context.Background(),
		`
		SELECT
			request_id,
			status
		FROM delivery_attempts
		WHERE order_id = $1
		`,
		createdOrder.ID,
	).Scan(
		&requestID,
		&status,
	)
	if err != nil {
		t.Fatalf(
			"query repaired delivery job: %v",
			err,
		)
	}

	expectedRequestID := fmt.Sprintf(
		"req-%s-1",
		createdOrder.ID,
	)

	if requestID != expectedRequestID {
		t.Fatalf(
			"expected request_id %s, got %s",
			expectedRequestID,
			requestID,
		)
	}

	if status != "pending" {
		t.Fatalf(
			"expected pending, got %s",
			status,
		)
	}
}
