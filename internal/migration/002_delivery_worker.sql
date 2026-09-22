CREATE INDEX IF NOT EXISTS delivery_attempts_worker_idx
    ON delivery_attempts (
                          next_attempt_at,
                          created_at
        )
    WHERE status IN (
                     'pending',
                     'failed',
                     'out_of_stock'
        );

CREATE INDEX IF NOT EXISTS provider_issues_request_idx
    ON provider_issues(provider, request_id);

CREATE INDEX IF NOT EXISTS provider_keys_request_idx
    ON provider_keys(provider, request_id)
    WHERE request_id IS NOT NULL;