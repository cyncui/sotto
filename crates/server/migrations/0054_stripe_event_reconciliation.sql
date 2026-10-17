-- Durable account-level scan windows for repairing Stripe events that were not delivered by
-- the signed webhook. A watermark is never moved to the right of an unfinished fixed window.

CREATE TABLE stripe_event_reconciliation_state (
    provider_account_id       TEXT NOT NULL CHECK (btrim(provider_account_id) <> ''),
    provider_environment      TEXT NOT NULL CHECK (provider_environment IN ('test', 'live')),
    completed_through         BIGINT CHECK (completed_through IS NULL OR completed_through >= 0),
    window_start               BIGINT CHECK (window_start IS NULL OR window_start >= 0),
    window_end                 BIGINT CHECK (window_end IS NULL OR window_end >= 0),
    starting_after             TEXT,
    lease_owner                TEXT,
    lease_expires_at           TIMESTAMPTZ,
    history_gap_start          BIGINT CHECK (history_gap_start IS NULL OR history_gap_start >= 0),
    history_gap_end            BIGINT CHECK (history_gap_end IS NULL OR history_gap_end >= 0),
    oldest_unresolved_created  BIGINT CHECK (oldest_unresolved_created IS NULL OR oldest_unresolved_created >= 0),
    last_outcome                TEXT CHECK (last_outcome IS NULL OR last_outcome IN
                                ('complete', 'retryable_failure', 'history_gap', 'bound_exceeded', 'unresolved')),
    last_error_code             TEXT CHECK (last_error_code IS NULL OR
                                last_error_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    last_attempt_at             TIMESTAMPTZ,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider_account_id, provider_environment),
    CONSTRAINT stripe_event_reconciliation_window_check
        CHECK ((window_start IS NULL) = (window_end IS NULL)
            AND (window_start IS NULL OR window_end >= window_start)),
    CONSTRAINT stripe_event_reconciliation_lease_pair_check
        CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
    CONSTRAINT stripe_event_reconciliation_gap_check
        CHECK ((history_gap_start IS NULL) = (history_gap_end IS NULL)
            AND (history_gap_start IS NULL OR history_gap_end >= history_gap_start))
);

CREATE TABLE stripe_event_repair_receipts (
    provider_account_id    TEXT NOT NULL CHECK (btrim(provider_account_id) <> ''),
    provider_environment   TEXT NOT NULL CHECK (provider_environment IN ('test', 'live')),
    event_id               TEXT NOT NULL CHECK (btrim(event_id) <> ''),
    event_type             TEXT NOT NULL CHECK (event_type IN ('invoice.paid', 'invoice.payment_failed')),
    provider_created_at    BIGINT NOT NULL CHECK (provider_created_at >= 0),
    retrieval_status       TEXT NOT NULL CHECK (retrieval_status IN ('retrieved', 'detail_unavailable')),
    api_version            TEXT CHECK (api_version IS NULL OR btrim(api_version) <> ''),
    payload_hash           TEXT CHECK (payload_hash IS NULL OR payload_hash ~ '^[0-9a-f]{64}$'),
    retrieved_at            BIGINT NOT NULL CHECK (retrieved_at >= 0),
    disposition             TEXT NOT NULL CHECK (disposition IN
                                ('applied', 'already_applied', 'unrelated', 'rejected', 'unresolved')),
    reason_code             TEXT CHECK (reason_code IS NULL OR reason_code ~ '^[a-z][a-z0-9_]{0,63}$'),
    received_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (provider_account_id, provider_environment, event_id),
    CONSTRAINT stripe_event_repair_reason_check
        CHECK ((disposition IN ('rejected', 'unresolved') AND reason_code IS NOT NULL)
            OR (disposition IN ('applied', 'already_applied', 'unrelated') AND reason_code IS NULL)),
    CONSTRAINT stripe_event_repair_payload_provenance_check
        CHECK ((retrieval_status = 'retrieved' AND api_version IS NOT NULL AND payload_hash IS NOT NULL)
            OR (retrieval_status = 'detail_unavailable' AND api_version IS NULL AND payload_hash IS NULL
                AND disposition = 'unresolved'))
);

CREATE INDEX stripe_event_repair_receipts_created_idx
    ON stripe_event_repair_receipts
        (provider_account_id, provider_environment, provider_created_at, event_id);
