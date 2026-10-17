//! Shared worker executor for a beneficiary whose registered history may mix personal and
//! sponsored Stripe sources.
//!
//! A ticket is prepared once for the beneficiary's complete source set. Each provider adapter
//! reads only the sources it owns, and the resulting observations are merged before one atomic
//! reconciliation completion.

use std::collections::BTreeMap;

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use crate::cloud_coverage_reconciliation::{CollectionTicket, SourceBinding};
use crate::cloud_provider::{
    collect_provider_history, complete_verified_event, prepare_verified_event, AllocationState,
    PayerKind, ProviderContext, VerifiedAllocation, VerifiedCollection,
};
use crate::cloud_provider_refresh_inputs::RefreshJobInputs;
use crate::cloud_provider_refresh_worker::{RefreshExecutionError, RefreshJobExecutor};
use crate::cloud_provider_stripe::StripeCoverageConfig;
use crate::cloud_provider_stripe_adapter::{
    prepared_source, StripeHistoryClient, StripePreparedCollection,
};
use crate::cloud_provider_stripe_http::StripeReadClient;
use crate::cloud_provider_stripe_sponsored::SponsoredStripeCoverageConfig;
use crate::cloud_provider_stripe_sponsored_refresh::{
    collect_sponsored_collection, SponsoredRefreshLimits,
};

#[derive(Clone)]
pub struct StripeRefreshExecutor {
    pool: PgPool,
    stripe: StripeReadClient,
    personal_coverage: StripeCoverageConfig,
    sponsored_coverage: SponsoredStripeCoverageConfig,
    sponsored_limits: SponsoredRefreshLimits,
}

impl StripeRefreshExecutor {
    pub fn new(
        pool: PgPool,
        stripe: StripeReadClient,
        personal_coverage: StripeCoverageConfig,
        sponsored_coverage: SponsoredStripeCoverageConfig,
        sponsored_limits: SponsoredRefreshLimits,
    ) -> Self {
        Self {
            pool,
            stripe,
            personal_coverage,
            sponsored_coverage,
            sponsored_limits,
        }
    }

    async fn execute_inner(&self, inputs: &RefreshJobInputs) -> Result<(), &'static str> {
        let context = &inputs.lease.context;
        if !refresh_input_matches_stripe(
            context,
            &inputs.event.event_type,
            &self.personal_coverage,
            &self.sponsored_coverage,
        ) {
            return Err("refresh_input_invalid");
        }

        let run_id = format!("stripe-refresh:{}", Uuid::new_v4());
        let mut prepare = self.pool.begin().await.map_err(|_| "refresh_database")?;
        let preparation = match prepare_verified_event(
            &mut prepare,
            context,
            &inputs.event,
            &inputs.allocation,
            &run_id,
        )
        .await
        {
            Ok(preparation) => preparation,
            Err(_) => {
                prepare.rollback().await.map_err(|_| "refresh_database")?;
                return Err("refresh_prepare");
            }
        };
        prepare.commit().await.map_err(|_| "refresh_database")?;

        // Resolve provider ownership from immutable local bindings after capturing the ticket.
        // These reads finish before any Stripe request; no transaction or connection spans I/O.
        let details = load_source_details(&self.pool, context, &preparation.ticket).await?;
        let collection = self
            .collect_mixed(context, &inputs.event, &preparation.ticket, &details)
            .await?;

        let mut complete = self.pool.begin().await.map_err(|_| "refresh_database")?;
        let result = complete_verified_event(
            &mut complete,
            context,
            &inputs.event,
            &inputs.allocation,
            &preparation,
            &collection,
        )
        .await;
        match result {
            Ok(_) => complete.commit().await.map_err(|_| "refresh_database"),
            Err(_) => {
                complete.rollback().await.map_err(|_| "refresh_database")?;
                Err("refresh_complete")
            }
        }
    }

    async fn collect_mixed(
        &self,
        context: &ProviderContext,
        event: &crate::cloud_provider::VerifiedProviderEvent,
        ticket: &CollectionTicket,
        details: &[SourceDetails],
    ) -> Result<VerifiedCollection, &'static str> {
        if details.len() != ticket.source_bindings.len() || details.is_empty() {
            return Err("refresh_source_set");
        }
        let mut observations = BTreeMap::new();
        let mut evidence_references = Vec::new();

        let personal = details
            .iter()
            .filter(|detail| detail.allocation.payer_kind == PayerKind::Personal)
            .collect::<Vec<_>>();
        if !personal.is_empty() {
            let personal_ticket = filtered_ticket(
                ticket,
                personal
                    .iter()
                    .map(|detail| detail.binding.clone())
                    .collect(),
            );
            let sources = personal
                .iter()
                .map(|detail| {
                    let allocation = crate::cloud_provider_stripe::StripeAllocationBinding::new(
                        detail.allocation.external_allocation_reference.clone(),
                        detail.allocation.provider_customer_id.clone(),
                        detail.allocation.subscription_id.clone(),
                        detail.allocation.provider_item_id.clone(),
                        PayerKind::Personal,
                    )
                    .map_err(|_| "refresh_personal_binding")?;
                    Ok(prepared_source(
                        detail.binding.clone(),
                        allocation,
                        detail.source_generation,
                        detail.allocation.ownership_evidence_reference.clone(),
                    ))
                })
                .collect::<Result<Vec<_>, &'static str>>()?;
            let prepared = StripePreparedCollection::from_ticket(
                &self.personal_coverage,
                &personal_ticket,
                sources,
            )
            .map_err(|_| "refresh_personal_binding")?;
            let mut renewal_failures = Vec::new();
            for detail in &personal {
                let failures =
                    crate::cloud_provider_stripe_renewal_store::load_personal_renewal_failures(
                        &self.pool,
                        context,
                        &detail.allocation,
                        Default::default(),
                    )
                    .await
                    .map_err(|_| "refresh_personal_renewals")?;
                renewal_failures.extend(failures);
            }
            let mut client = StripeHistoryClient::new(self.stripe.clone(), prepared)
                .with_renewal_failures(renewal_failures);
            let collection = collect_provider_history(
                &mut client,
                context,
                &personal_ticket.source_bindings,
                Default::default(),
            )
            .await
            .map_err(|_| "refresh_personal_evidence")?;
            add_observations(&mut observations, &mut evidence_references, collection)?;
        }

        let mut sponsored_groups = BTreeMap::<(String, String), Vec<&SourceDetails>>::new();
        for detail in details
            .iter()
            .filter(|detail| detail.allocation.payer_kind == PayerKind::Sponsor)
        {
            sponsored_groups
                .entry((
                    detail.allocation.subscription_id.clone(),
                    detail.allocation.provider_customer_id.clone(),
                ))
                .or_default()
                .push(detail);
        }
        for group in sponsored_groups.values() {
            let sponsor_ticket = filtered_ticket(
                ticket,
                group.iter().map(|detail| detail.binding.clone()).collect(),
            );
            let anchor = &group[0].allocation;
            let collection = if event.subscription_id.as_deref()
                == Some(anchor.subscription_id.as_str())
            {
                collect_sponsored_collection(
                    &self.pool,
                    &self.stripe,
                    &self.sponsored_coverage,
                    context,
                    event,
                    anchor,
                    &sponsor_ticket,
                    self.sponsored_limits,
                )
                .await
            } else {
                crate::cloud_provider_stripe_sponsored_refresh::collect_sponsored_subscription_collection(
                    &self.pool,
                    &self.stripe,
                    &self.sponsored_coverage,
                    context,
                    anchor,
                    &sponsor_ticket,
                    self.sponsored_limits,
                )
                .await
            }
            .map_err(|_| "refresh_sponsored_evidence")?;
            add_observations(&mut observations, &mut evidence_references, collection)?;
        }

        if observations.len() != ticket.source_bindings.len() {
            return Err("refresh_source_set");
        }
        let evidence_material = evidence_references.join("\n");
        let digest = Sha256::digest(evidence_material.as_bytes());
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(VerifiedCollection {
            aggregate_evidence_reference: format!("stripe-mixed-collection-v1:{digest}"),
            observations: ticket
                .source_bindings
                .iter()
                .map(|binding| {
                    observations
                        .remove(&binding.source_id)
                        .ok_or("refresh_source_set")
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

fn refresh_input_matches_stripe(
    context: &ProviderContext,
    event_type: &str,
    personal_coverage: &StripeCoverageConfig,
    sponsored_coverage: &SponsoredStripeCoverageConfig,
) -> bool {
    matches!(event_type, "invoice.paid" | "invoice.payment_failed")
        && context.namespace == crate::cloud_provider_stripe::STRIPE_NAMESPACE
        && context.account_id == personal_coverage.account_id
        && context.account_id == sponsored_coverage.account_id()
        && context.environment == personal_coverage.environment
        && context.environment == sponsored_coverage.environment()
}

#[async_trait]
impl RefreshJobExecutor for StripeRefreshExecutor {
    async fn execute(&mut self, inputs: &RefreshJobInputs) -> Result<(), RefreshExecutionError> {
        self.execute_inner(inputs).await.map_err(|code| {
            RefreshExecutionError::new(code).expect("static refresh error codes are nonempty")
        })
    }
}

pub fn spawn(
    pool: PgPool,
    stripe: StripeReadClient,
    personal_coverage: StripeCoverageConfig,
    sponsored_coverage: SponsoredStripeCoverageConfig,
) {
    tokio::spawn(async move {
        let worker_id = format!("stripe-refresh:{}", Uuid::new_v4());
        let mut executor = StripeRefreshExecutor::new(
            pool.clone(),
            stripe,
            personal_coverage,
            sponsored_coverage,
            SponsoredRefreshLimits::default(),
        );
        loop {
            match crate::cloud_provider_refresh_worker::run_once(&pool, &worker_id, &mut executor)
                .await
            {
                Ok(crate::cloud_provider_refresh_worker::RefreshWorkerOutcome::Idle) => {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }
                Ok(_) => {}
                Err(_) => {
                    eprintln!("warning: Stripe coverage refresh worker iteration failed");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    });
}

struct SourceDetails {
    binding: SourceBinding,
    allocation: VerifiedAllocation,
    source_generation: i64,
}

async fn load_source_details(
    pool: &PgPool,
    context: &ProviderContext,
    ticket: &CollectionTicket,
) -> Result<Vec<SourceDetails>, &'static str> {
    let mut details = Vec::with_capacity(ticket.source_bindings.len());
    for binding in &ticket.source_bindings {
        let row = sqlx::query(
            "SELECT allocation.allocation_id, allocation.payer_id, payer.provider_customer_id, \
                    payer.payer_kind, allocation.provider_subscription_id, allocation.provider_item_id, \
                    allocation.external_allocation_reference, allocation.effective_from, \
                    allocation.effective_until, allocation.state, allocation.ownership_evidence_reference, \
                    source.registration_source_set_generation \
             FROM cloud_provider_allocations AS allocation \
             JOIN cloud_provider_payers AS payer ON payer.payer_id = allocation.payer_id \
             JOIN cloud_coverage_sources AS source ON source.source_id = allocation.coverage_source_id \
             WHERE allocation.coverage_source_id = $1 AND allocation.beneficiary_id = $2 \
               AND allocation.provider_namespace = $3 AND allocation.provider_account_id = $4 \
               AND allocation.provider_environment = $5",
        )
        .bind(&binding.source_id)
        .bind(&binding.beneficiary_id)
        .bind(&context.namespace)
        .bind(&context.account_id)
        .bind(context.environment.as_str())
        .fetch_optional(pool)
        .await
        .map_err(|_| "refresh_database")?
        .ok_or("refresh_source_missing")?;
        let payer_kind = match row
            .try_get::<String, _>("payer_kind")
            .map_err(|_| "refresh_source_corrupt")?
            .as_str()
        {
            "personal" => PayerKind::Personal,
            "sponsor" => PayerKind::Sponsor,
            _ => return Err("refresh_source_corrupt"),
        };
        let state = match row
            .try_get::<String, _>("state")
            .map_err(|_| "refresh_source_corrupt")?
            .as_str()
        {
            "pending" => AllocationState::Pending,
            "active" => AllocationState::Active,
            "ended" => AllocationState::Ended,
            _ => return Err("refresh_source_corrupt"),
        };
        let external_reference: String = row
            .try_get("external_allocation_reference")
            .map_err(|_| "refresh_source_corrupt")?;
        if external_reference != binding.external_allocation_reference {
            return Err("refresh_source_corrupt");
        }
        let allocation = VerifiedAllocation::new(
            row.try_get::<String, _>("allocation_id")
                .map_err(|_| "refresh_source_corrupt")?,
            row.try_get::<String, _>("payer_id")
                .map_err(|_| "refresh_source_corrupt")?,
            row.try_get::<String, _>("provider_customer_id")
                .map_err(|_| "refresh_source_corrupt")?,
            payer_kind,
            ticket.beneficiary_id.clone(),
            row.try_get::<String, _>("provider_subscription_id")
                .map_err(|_| "refresh_source_corrupt")?,
            row.try_get::<String, _>("provider_item_id")
                .map_err(|_| "refresh_source_corrupt")?,
            external_reference,
            binding.source_id.clone(),
            row.try_get("effective_from")
                .map_err(|_| "refresh_source_corrupt")?,
            row.try_get("effective_until")
                .map_err(|_| "refresh_source_corrupt")?,
            state,
            row.try_get::<String, _>("ownership_evidence_reference")
                .map_err(|_| "refresh_source_corrupt")?,
        )
        .map_err(|_| "refresh_source_corrupt")?;
        details.push(SourceDetails {
            binding: binding.clone(),
            allocation,
            source_generation: row
                .try_get("registration_source_set_generation")
                .map_err(|_| "refresh_source_corrupt")?,
        });
    }
    Ok(details)
}

fn filtered_ticket(
    ticket: &CollectionTicket,
    source_bindings: Vec<SourceBinding>,
) -> CollectionTicket {
    let mut filtered = ticket.clone();
    filtered.source_bindings = source_bindings;
    filtered
}

fn add_observations(
    observations: &mut BTreeMap<String, crate::cloud_coverage_reconciliation::SourceObservation>,
    evidence_references: &mut Vec<String>,
    collection: VerifiedCollection,
) -> Result<(), &'static str> {
    evidence_references.push(collection.aggregate_evidence_reference);
    for observation in collection.observations {
        let source_id = match &observation {
            crate::cloud_coverage_reconciliation::SourceObservation::Complete {
                source_id, ..
            }
            | crate::cloud_coverage_reconciliation::SourceObservation::Unavailable {
                source_id,
                ..
            } => source_id.clone(),
        };
        if observations.insert(source_id, observation).is_some() {
            return Err("refresh_duplicate_source");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn personal_config() -> StripeCoverageConfig {
        StripeCoverageConfig::new(
            "acct_test",
            crate::cloud_provider::ProviderEnvironment::Test,
            "price_month",
            "price_year",
        )
        .unwrap()
    }

    fn sponsored_config() -> SponsoredStripeCoverageConfig {
        SponsoredStripeCoverageConfig::new(
            "acct_test",
            crate::cloud_provider::ProviderEnvironment::Test,
            "price_month",
            "price_year",
            "price_found_month",
            "price_found_year",
        )
        .unwrap()
    }

    #[test]
    fn refresh_input_rejects_other_provider_namespaces_before_preparation() {
        let context = ProviderContext::new(
            "other-provider",
            "acct_test",
            crate::cloud_provider::ProviderEnvironment::Test,
        )
        .unwrap();

        assert!(!refresh_input_matches_stripe(
            &context,
            "invoice.paid",
            &personal_config(),
            &sponsored_config(),
        ));
    }
}
