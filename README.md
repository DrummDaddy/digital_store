# Digital Store Core

Backend-ядро магазина цифровых товаров на Go и PostgreSQL.

Проект реализует:

- создание заказа по SKU;
- приём payment webhook;
- идемпотентность платежных событий;
- автоматическую выдачу товара;
- двух mock-поставщиков;
- retry с backoff;
- fallback между поставщиками;
- идемпотентную выдачу по request_id;
- обработку таймаута после фактической выдачи;
- PostgreSQL worker queue;
- сверку состояния заказов;
- восстановление зависших задач;
- каталог с keyset pagination;
- интеграционные тесты критичных сценариев.

## Стек

- Go 1.24+
- PostgreSQL 16
- net/http
- chi
- pgx/v5
- slog
- Docker Compose

## Запуск через Docker Compose

Полностью пересоздать локальную БД:

```bash
docker compose down -v
docker compose up --build 
``` 
## API 
``` http://localhost:8080 ```
## Health checks 
```bash
   curl http://localhost:8080/health/live  
   curl http://localhost:8080/health/ready
   ```

## Создание заказа 
```curl -X POST http://localhost:8080/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"STEAM-TOPUP-500"}' 
```
## Оплата заказа 

```
   curl -X POST http://localhost:8080/api/webhooks/payment \
  -H 'Content-Type: application/json' \
  -d '{
  "event_id":"evt-001",
  "order_id":"ORDER_ID",
  "status":"paid",
  "amount":500,
  "currency":"RUB",
  "created_at":"2026-09-25T10:00:00Z"
  }'  
  ```
## Получение заказа 
````
 curl http://localhost:8080/api/orders/ORDER_ID 
````
## Каталог 
````
 curl 'http://localhost:8080/api/catalog?limit=5' 
````
## Следующая страница 
```
 curl 'http://localhost:8080/api/catalog?limit=5&after=LAST_SKU'
```
- Используется keyset pagination по SKU.

## Сверка
```
 curl http://localhost:8080/api/admin/reconciliation 
```
## Запустить восстановление 
```
 curl -X POST \
  http://localhost:8080/api/admin/reconciliation/repair 
```
## Безопасно повторить выдачу 
```  
  curl -X POST \
  http://localhost:8080/api/admin/orders/ORDER_ID/retry-delivery
```
## Тесты 

- Для unit-тестов:
```` 
go test ./... 
````
- Для интеграционных тестов:

```` 
export TEST_DATABASE_URL='postgres://digital_store:digital_store@localhost:5432/digital_store_test?sslmode=disable'

go test -race ./tests -v -count=1
```` 
- Основные тесты:
```` 
go test -race ./tests \
  -run '^TestFiftyParallelPaymentWebhooksExactlyOnce$' \
  -v -count=1

go test -race ./tests \
  -run '^TestDuplicateEventIDIsIdempotent$' \
  -v -count=1

go test ./tests \
  -run '^TestPaymentWebhookBeforeOrder$' \
  -v -count=1

go test ./tests \
  -run '^TestTimeoutAfterIssueReturnsSameCode$' \
  -v -count=1

go test ./tests \
  -run '^TestFallbackFromProviderAToProviderB$' \
  -v -count=1

go test ./tests \
  -run '^TestOutOfStockIsRecoverable$' \
  -v -count=1 
```` 

## Проверка каталога
```` 
TEST_DATABASE_URL='postgres://digital_store:digital_store@localhost:5432/digital_store_test?sslmode=disable' \
go test ./tests \
  -bench BenchmarkCatalogList \
  -benchmem \
  -count=3 
```` 
План запроса:
````
EXPLAIN (ANALYZE, BUFFERS)
SELECT
p.sku,
p.name,
p.product_type,
p.price_minor,
p.currency,
p.image,
COALESCE(i.available, 0)
FROM products AS p
LEFT JOIN inventory AS i
ON i.sku = p.sku
WHERE p.active = TRUE
AND p.sku > 'GIFT-PSN-1000'
ORDER BY p.sku
LIMIT 50;
```` 

## Exactly-once и идемпотентность

- Платёжный webhook защищён уникальным payment_events.event_id.

- Одна выдача на заказ обеспечивается уникальным ограничением:

```` 
delivery_attempts(order_id) 
```` 
- Внешний поставщик получает стабильный request_id. Повтор с тем же request_id должен вернуть тот же код.

- Важно: timeout не считается подтверждённым отказом. Поставщик мог выдать код до того, как клиент получил timeout. Поэтому fallback после timeout не выполняется автоматически.

##  Статусы заказа

- Основной путь:

```` 
created → paid → delivering → delivered 
```` 

- Восстанавливаемые состояния:
````
paid → delivering → out_of_stock
paid → delivering → delivery_failed 
```` 
- Финальные состояния:

```` 
delivered
payment_failed 

```` 

## Масштабирование 

- API можно масштабировать горизонтально, потому что состояние хранится в PostgreSQL.

- Worker-ы используют:

```` 
FOR UPDATE SKIP LOCKED 
```` 
- При росте нагрузки очередь можно вынести в RabbitMQ, NATS или Kafka, но PostgreSQL должен оставаться источником истины по состоянию заказа.


## Каталог использует

- partial index по активным товарам;
- covering index;
- keyset pagination;
- отдельную таблицу inventory;
- индексы по SKU.
- При дальнейшем росте можно добавить:

- Redis-кэш каталога;
- read replica;
- CDN;
- materialized view;
- отдельный сервис каталога. 

# Финальная проверка проекта 

Выполни:

```bash
gofmt -w cmd internal tests
go mod tidy
go test ./...
go vet ./... 
``` 
Проверка сборки: 
```` 
go build ./cmd/api
go build ./cmd/provider-mock 

```` 
Проверка race detector:
```` 
go test -race ./... 
```` 

Проверка Docker:
````
docker compose down -v
docker compose up --build 

```` 
Проверка endpoint-ов:

```` 
curl -f http://localhost:8080/health/live
curl -f http://localhost:8080/health/ready
curl -f http://localhost:8080/api/catalog?limit=5 
````


