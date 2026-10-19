-- Give each new personal Checkout a durable opaque identity that Stripe can echo on invoices.
-- Existing rows receive local references, but remain unbound because Stripe cannot be updated
-- retroactively; they keep billing normally and become coverage-bound on their next Checkout.
ALTER TABLE billing_personal_accounts
    ADD COLUMN allocation_reference TEXT NOT NULL
        DEFAULT ('alloc_' || gen_random_uuid()::TEXT),
    -- Existing Stripe subscriptions cannot be backfilled with this binding. New checkouts opt
    -- in explicitly; legacy billing remains valid but does not produce Cloud coverage evidence.
    ADD COLUMN stripe_coverage_binding_required BOOLEAN NOT NULL DEFAULT FALSE,
    ADD CONSTRAINT billing_personal_accounts_allocation_reference_key
        UNIQUE (allocation_reference),
    ADD CONSTRAINT billing_personal_accounts_allocation_reference_nonempty
        CHECK (btrim(allocation_reference) <> '');
