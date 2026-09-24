CREATE UNIQUE INDEX IF NOT EXISTS ledger_delivery_once_idx
    ON ledger_entries(order_id, entry_type)
    WHERE entry_type = 'delivery';