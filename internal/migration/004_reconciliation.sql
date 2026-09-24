CREATE INDEX IF NOT EXISTS orders_paid_not_delivered_idx
    ON orders(status, paid_at, created_at)
    WHERE status IN (
                     'paid',
                     'delivering',
                     'out_of_stock',
                     'delivery_failed'
        );

CREATE INDEX IF NOT EXISTS orders_delivered_idx
    ON orders(delivered_at, created_at)
    WHERE status = 'delivered';

CREATE INDEX IF NOT EXISTS ledger_order_type_idx
    ON ledger_entries(order_id, entry_type);

CREATE INDEX IF NOT EXISTS delivery_attempts_processing_locked_idx
    ON delivery_attempts(locked_at)
    WHERE status = 'processing';