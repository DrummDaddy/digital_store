CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_type
            WHERE typname = 'order_status'
        ) THEN
            CREATE TYPE order_status AS ENUM (
                'created',
                'paid',
                'delivering',
                'delivered',
                'payment_failed',
                'out_of_stock',
                'delivery_failed'
                );
        END IF;
    END
$$;

CREATE TABLE IF NOT EXISTS products (
                                        sku TEXT PRIMARY KEY,
                                        name TEXT NOT NULL,
                                        product_type TEXT NOT NULL,
                                        price_minor BIGINT NOT NULL CHECK (price_minor >= 0),
                                        currency CHAR(3) NOT NULL,
                                        image TEXT,
                                        active BOOLEAN NOT NULL DEFAULT TRUE,
                                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory (
                                         sku TEXT PRIMARY KEY REFERENCES products(sku),
                                         available BIGINT NOT NULL DEFAULT 0 CHECK (available >= 0),
                                         reserved BIGINT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
                                         updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
                                      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                      sku TEXT NOT NULL REFERENCES products(sku),
                                      amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
                                      currency CHAR(3) NOT NULL,
                                      status order_status NOT NULL DEFAULT 'created',
                                      paid_at TIMESTAMPTZ,
                                      delivered_at TIMESTAMPTZ,
                                      last_error TEXT,
                                      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS payment_events (
                                              event_id TEXT PRIMARY KEY,
                                              order_id UUID NOT NULL,
                                              status TEXT NOT NULL CHECK (status IN ('paid', 'failed')),
                                              amount_minor BIGINT NOT NULL CHECK (amount_minor >= 0),
                                              currency CHAR(3) NOT NULL,
                                              payload JSONB NOT NULL,
                                              received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                              processed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS payment_events_order_id_idx
    ON payment_events(order_id);

CREATE INDEX IF NOT EXISTS payment_events_unprocessed_idx
    ON payment_events(order_id, received_at)
    WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS delivery_attempts (
                                                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                                 order_id UUID NOT NULL REFERENCES orders(id),
                                                 request_id TEXT NOT NULL,
                                                 provider TEXT,
                                                 status TEXT NOT NULL CHECK (
                                                     status IN (
                                                                'pending',
                                                                'processing',
                                                                'delivered',
                                                                'out_of_stock',
                                                                'failed'
                                                         )
                                                     ),
                                                 code TEXT,
                                                 error TEXT,
                                                 attempts INTEGER NOT NULL DEFAULT 0,
                                                 next_attempt_at TIMESTAMPTZ,
                                                 locked_at TIMESTAMPTZ,
                                                 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                                 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                                                 CONSTRAINT delivery_attempts_order_unique UNIQUE (order_id),
                                                 CONSTRAINT delivery_attempts_request_unique UNIQUE (request_id)
);

CREATE INDEX IF NOT EXISTS delivery_attempts_queue_idx
    ON delivery_attempts(status, next_attempt_at, created_at);

CREATE TABLE IF NOT EXISTS provider_issues (
                                               provider TEXT NOT NULL,
                                               request_id TEXT NOT NULL,
                                               sku TEXT NOT NULL,
                                               order_id UUID NOT NULL,
                                               code TEXT,
                                               status TEXT NOT NULL CHECK (
                                                   status IN (
                                                              'processing',
                                                              'issued',
                                                              'out_of_stock',
                                                              'failed'
                                                       )
                                                   ),
                                               created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                               updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                                               PRIMARY KEY (provider, request_id)
);

CREATE TABLE IF NOT EXISTS ledger_entries (
                                              id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                              order_id UUID NOT NULL REFERENCES orders(id),
                                              entry_type TEXT NOT NULL CHECK (
                                                  entry_type IN ('payment', 'delivery', 'refund')
                                                  ),
                                              amount_minor BIGINT NOT NULL,
                                              currency CHAR(3) NOT NULL,
                                              created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ledger_payment_once_idx
    ON ledger_entries(order_id, entry_type)
    WHERE entry_type = 'payment';

CREATE INDEX IF NOT EXISTS products_active_sku_idx
    ON products(sku)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS products_active_price_idx
    ON products(price_minor, sku)
    WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS inventory_available_idx
    ON inventory(available, sku)
    WHERE available > 0;

CREATE TABLE IF NOT EXISTS provider_keys (
                                             id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                             provider TEXT NOT NULL,
                                             sku TEXT NOT NULL REFERENCES products(sku),
                                             code TEXT NOT NULL,
                                             issued BOOLEAN NOT NULL DEFAULT FALSE,
                                             request_id TEXT,
                                             order_id UUID,
                                             issued_at TIMESTAMPTZ,
                                             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

                                             CONSTRAINT provider_keys_code_unique UNIQUE (code)
);

CREATE INDEX IF NOT EXISTS provider_keys_available_idx
    ON provider_keys(provider, sku, issued, created_at)
    WHERE issued = FALSE;