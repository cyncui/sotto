-- One provider event can invalidate and refresh more than one beneficiary. Keep the provider
-- event receipt global, but move collection ownership and completion state to beneficiary scopes.

CREATE TABLE cloud_provider_event_scopes (
    provider_namespace       TEXT NOT NULL,
    provider_account_id      TEXT NOT NULL,
    provider_environment     TEXT NOT NULL,
    event_id                 TEXT NOT NULL,
    beneficiary_id           TEXT NOT NULL CHECK (btrim(beneficiary_id) <> ''),
    status                   TEXT NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'applied', 'rejected')),
    rejection_code           TEXT,
    projection_revision      BIGINT CHECK (projection_revision IS NULL OR projection_revision > 0),
    collection_attempt_id    TEXT,
    collection_run_id        TEXT,
    received_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at             TIMESTAMPTZ,
    PRIMARY KEY (provider_namespace, provider_account_id, provider_environment, event_id, beneficiary_id),
    CONSTRAINT cloud_provider_event_scope_event_fk
        FOREIGN KEY (provider_namespace, provider_account_id, provider_environment, event_id)
        REFERENCES cloud_provider_event_receipts
            (provider_namespace, provider_account_id, provider_environment, event_id)
        ON DELETE CASCADE,
    CONSTRAINT cloud_provider_event_scope_collection_pair_check
        CHECK ((collection_attempt_id IS NULL) = (collection_run_id IS NULL)),
    CONSTRAINT cloud_provider_event_scope_terminal_check
        CHECK (
            (status = 'pending' AND processed_at IS NULL AND rejection_code IS NULL
                AND projection_revision IS NULL)
            OR (status = 'applied' AND processed_at IS NOT NULL AND rejection_code IS NULL
                AND projection_revision IS NOT NULL)
            OR (status = 'rejected' AND processed_at IS NOT NULL AND rejection_code IS NOT NULL
                AND projection_revision IS NULL)
        )
);

-- Existing queue rows represent one event/beneficiary each. Preserve their retry state and
-- collection association while moving future work to the scoped table.
INSERT INTO cloud_provider_event_scopes (
    provider_namespace, provider_account_id, provider_environment, event_id,
    beneficiary_id, status, rejection_code,
    projection_revision, collection_attempt_id, collection_run_id, processed_at
)
SELECT job.provider_namespace, job.provider_account_id, job.provider_environment, job.event_id,
       job.beneficiary_id,
       CASE WHEN receipt.status = 'applied'
                  AND receipt.collection_beneficiary_id = job.beneficiary_id
            THEN 'applied' ELSE 'pending' END,
       NULL,
       CASE WHEN receipt.status = 'applied'
                  AND receipt.collection_beneficiary_id = job.beneficiary_id
            THEN receipt.projection_revision ELSE NULL END,
       CASE WHEN receipt.collection_beneficiary_id = job.beneficiary_id
            THEN receipt.collection_attempt_id ELSE NULL END,
       CASE WHEN receipt.collection_beneficiary_id = job.beneficiary_id
            THEN receipt.collection_run_id ELSE NULL END,
       CASE WHEN receipt.status = 'applied'
                  AND receipt.collection_beneficiary_id = job.beneficiary_id
            THEN receipt.processed_at ELSE NULL END
FROM cloud_provider_refresh_jobs AS job
JOIN cloud_provider_event_receipts AS receipt
  USING (provider_namespace, provider_account_id, provider_environment, event_id)
ON CONFLICT DO NOTHING;

ALTER TABLE cloud_provider_stripe_renewal_failures
    DROP CONSTRAINT cloud_provider_stripe_renewal_failure_association_fk;

ALTER TABLE cloud_provider_stripe_renewal_failures
    DROP CONSTRAINT cloud_provider_stripe_renewal_failures_pkey;

ALTER TABLE cloud_provider_stripe_renewal_failures
    ADD CONSTRAINT cloud_provider_stripe_renewal_failures_pkey
        PRIMARY KEY (provider_namespace, provider_account_id, provider_environment,
                     event_id, beneficiary_id);

ALTER TABLE cloud_provider_invalidation_associations
    DROP CONSTRAINT cloud_provider_invalidation_associations_pkey;

ALTER TABLE cloud_provider_invalidation_associations
    ADD CONSTRAINT cloud_provider_invalidation_associations_pkey
        PRIMARY KEY (provider_namespace, provider_account_id, provider_environment,
                     event_id, beneficiary_id);

ALTER TABLE cloud_provider_stripe_renewal_failures
    ADD CONSTRAINT cloud_provider_stripe_renewal_failure_association_fk
        FOREIGN KEY (provider_namespace, provider_account_id, provider_environment,
                     event_id, beneficiary_id)
        REFERENCES cloud_provider_invalidation_associations
            (provider_namespace, provider_account_id, provider_environment, event_id,
             beneficiary_id)
        ON DELETE RESTRICT;

DROP INDEX cloud_provider_refresh_jobs_event_idx;

CREATE UNIQUE INDEX cloud_provider_refresh_jobs_event_beneficiary_idx
    ON cloud_provider_refresh_jobs
        (provider_namespace, provider_account_id, provider_environment, event_id, beneficiary_id);

CREATE INDEX cloud_provider_event_scopes_beneficiary_status_idx
    ON cloud_provider_event_scopes (beneficiary_id, status, received_at);
