-- A verified founding award needs a durable provider schedule before its standard-price
-- transition can be treated as promised. Jobs are person-scoped even when several people share
-- one sponsored subscription; each sponsored execution rebuilds the complete subscription plan.
ALTER TABLE billing_founding_awards
    ADD COLUMN original_start_epoch BIGINT,
    ADD COLUMN original_end_epoch BIGINT;

ALTER TABLE billing_founding_awards
    ADD CONSTRAINT billing_founding_award_epoch_pair CHECK (
        (original_start_epoch IS NULL AND original_end_epoch IS NULL)
        OR (original_start_epoch IS NOT NULL AND original_end_epoch IS NOT NULL
            AND original_start_epoch >= 0 AND original_end_epoch > original_start_epoch)
    );

CREATE TABLE billing_founding_schedule_jobs (
    job_id TEXT PRIMARY KEY,
    award_id TEXT NOT NULL UNIQUE REFERENCES billing_founding_awards (award_id) ON DELETE RESTRICT,
    beneficiary_id TEXT NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    payer_kind TEXT NOT NULL CHECK (payer_kind IN ('personal', 'sponsor')),
    payer_id TEXT NOT NULL,
    provider_subscription_id TEXT NOT NULL,
    provider_idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'scheduled', 'unresolved', 'not_required')),
    provider_schedule_id TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error_code TEXT CHECK (
        last_error_code IS NULL OR last_error_code IN (
            'provider_authentication', 'provider_conflict', 'provider_missing',
            'provider_retryable', 'provider_unknown', 'provider_malformed',
            'local_binding_missing', 'local_evidence_invalid',
            'subscription_terminates_before_transition', 'invoice_price_mismatch',
            'invoice_boundary_missing'
        )
    ),
    lease_token TEXT,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (btrim(job_id) <> ''),
    CHECK (btrim(payer_id) <> ''),
    CHECK (btrim(provider_subscription_id) <> ''),
    CHECK (btrim(provider_idempotency_key) <> ''),
    CHECK ((lease_token IS NULL) = (lease_until IS NULL)),
    CHECK ((state = 'scheduled') = (provider_schedule_id IS NOT NULL))
);

CREATE INDEX billing_founding_schedule_jobs_claim_idx
    ON billing_founding_schedule_jobs (next_attempt_at, created_at, job_id)
    WHERE state IN ('pending', 'unresolved');

-- Do not infer original billing anchors for awards created before this migration. Give current
-- subscriptions an explicit unresolved work item so the lifecycle view and operator inventory
-- cannot imply that a transition is scheduled from guessed dates.
INSERT INTO billing_founding_schedule_jobs (
    job_id, award_id, beneficiary_id, payer_kind, payer_id, provider_subscription_id,
    provider_idempotency_key, state, last_error_code
)
SELECT 'founding-schedule:' || award.award_id, award.award_id, award.beneficiary_id,
       award.payer_kind, award.payer_id, account.stripe_subscription_id,
       'founding-transition:' || award.award_id || ':1', 'unresolved', 'local_evidence_invalid'
FROM billing_founding_awards AS award
JOIN billing_personal_accounts AS account
  ON award.payer_kind = 'personal' AND award.payer_id = account.user_id
WHERE account.stripe_subscription_id IS NOT NULL
UNION ALL
SELECT 'founding-schedule:' || award.award_id, award.award_id, award.beneficiary_id,
       award.payer_kind, award.payer_id, subscription.provider_subscription_id,
       'founding-transition:' || award.award_id || ':1', 'unresolved', 'local_evidence_invalid'
FROM billing_founding_awards AS award
JOIN billing_sponsored_subscriptions AS subscription
  ON award.payer_kind = 'sponsor' AND award.payer_id = subscription.organization_id
WHERE subscription.provider_subscription_id IS NOT NULL;
