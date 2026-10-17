//! Durable bounded-window tests for missed Stripe event repair.

use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use axum::routing::any;
use axum::{body::Body, Router};
use serde_json::json;
use sotto_server::billing::STRIPE_API_VERSION;
use sotto_server::cloud_provider::ProviderEnvironment;
use sotto_server::cloud_provider_stripe::StripeCoverageConfig;
use sotto_server::cloud_provider_stripe_event_repair::{
    claim_window, fail_window, finish_page, record_event_disposition, renew_window_lease, run_once,
    status_snapshot, StripeEventRepairDisposition, StripeEventRepairError,
    StripeEventRepairPayloadProvenance, StripeEventRepairReceipt, StripeEventRepairRunOutcome,
};
use sotto_server::cloud_provider_stripe_http::{StripeReadClient, StripeReadLimits};
use sotto_server::db;
use sqlx::{PgPool, Row};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use url::Url;

mod support {
    #[allow(dead_code)]
    pub mod migrations;
}

use support::migrations::DisposableDatabase;

const NOW: i64 = 2_000_000_000;

#[derive(Clone)]
struct EventApiState {
    account_id: String,
    event_id: String,
    created: i64,
    detail_available: bool,
    detail_missing: bool,
    detail_missing_object: bool,
    calls: Arc<Mutex<Vec<String>>>,
}

struct EventApiServer {
    origin: Url,
    calls: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for EventApiServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn event_api_handler(
    State(state): State<EventApiState>,
    request: Request<Body>,
) -> Response<Body> {
    let path = request.uri().path().to_owned();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_owned())
        .unwrap_or(path.clone());
    state.calls.lock().unwrap().push(path_and_query);
    let value = match path.as_str() {
        "/v1/account" => json!({
            "object": "account",
            "id": state.account_id,
            "livemode": false
        }),
        "/v1/events" => json!({
            "object": "list",
            "has_more": false,
            "data": [{
                "id": state.event_id,
                "type": "invoice.paid",
                "created": state.created,
                "livemode": false
            }]
        }),
        path if path == format!("/v1/events/{}", state.event_id) => {
            if state.detail_missing {
                return Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap();
            }
            if state.detail_missing_object {
                json!({
                    "object": "event",
                    "id": state.event_id,
                    "type": "invoice.paid",
                    "created": state.created,
                    "api_version": STRIPE_API_VERSION,
                    "livemode": false,
                    "data": {}
                })
            } else if state.detail_available {
                json!({
                    "object": "event",
                    "id": state.event_id,
                    "type": "invoice.paid",
                    "created": state.created,
                    "api_version": STRIPE_API_VERSION,
                    "livemode": false,
                    "data": { "object": { "id": "in_scan_fixture", "object": "invoice" } }
                })
            } else {
                json!({
                    "object": "event",
                    "id": state.event_id,
                    "type": "invoice.paid",
                    "created": state.created,
                    "livemode": false
                })
            }
        }
        _ => {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&value).unwrap()))
        .unwrap()
}

async fn event_api_server(
    account_id: &str,
    event_id: &str,
    created: i64,
    detail_available: bool,
    detail_missing: bool,
    detail_missing_object: bool,
) -> EventApiServer {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let state = EventApiState {
        account_id: account_id.to_owned(),
        event_id: event_id.to_owned(),
        created,
        detail_available,
        detail_missing,
        detail_missing_object,
        calls: Arc::clone(&calls),
    };
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new()
        .fallback(any(event_api_handler))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    EventApiServer {
        origin: Url::parse(&format!("http://{address}/")).unwrap(),
        calls,
        task,
    }
}

fn stripe_config(account_id: &str) -> StripeCoverageConfig {
    StripeCoverageConfig::new(
        account_id,
        ProviderEnvironment::Test,
        "price_month",
        "price_year",
    )
    .expect("valid Stripe coverage config")
}

async fn migrate(pool: &PgPool) {
    db::migrate(pool).await.expect("apply all migrations");
}

#[tokio::test]
async fn scanner_retrieves_each_listed_event_and_checkpoints_its_disposition() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let event_id = format!("evt_{}", uuid::Uuid::new_v4().simple());
    let created = NOW - 30 * 24 * 60 * 60 + 100;
    let server = event_api_server(&account, &event_id, created, true, false, false).await;
    let config = stripe_config(&account);
    let stripe = StripeReadClient::for_test(
        "sk_test_event_repair",
        &config,
        server.origin.clone(),
        StripeReadLimits::default(),
    )
    .expect("construct bounded test Stripe client");
    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_for_apply = Arc::clone(&seen);
    let expected_event_id = event_id.clone();
    let expected_account = account.clone();

    let outcome = run_once(
        &pool,
        &stripe,
        &account,
        ProviderEnvironment::Test,
        NOW,
        move |event, _retrieved_at| {
            let seen = Arc::clone(&seen_for_apply);
            let expected_event_id = expected_event_id.clone();
            let expected_account = expected_account.clone();
            async move {
                assert_eq!(event.event_id(), expected_event_id);
                assert_eq!(event.account_id(), expected_account);
                assert_eq!(event.environment(), ProviderEnvironment::Test);
                assert_eq!(event.payload()["type"], "invoice.paid");
                seen.lock().unwrap().push(event.event_id().to_owned());
                Ok(StripeEventRepairDisposition::Applied)
            }
        },
    )
    .await
    .expect("scan and apply one authenticated event");
    assert!(matches!(
        outcome,
        StripeEventRepairRunOutcome::PageCompleted {
            events: 1,
            applied: 1,
            window_completed: true,
            ..
        }
    ));
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        std::slice::from_ref(&event_id)
    );
    let calls = server.calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 3);
    assert_eq!(calls[0], "/v1/account");
    assert!(calls[1].starts_with("/v1/events?"));
    assert!(calls[1].contains("types%5B%5D=invoice.paid"));
    assert!(calls[1].contains("created%5Bgte%5D="));
    assert_eq!(calls[2], format!("/v1/events/{event_id}"));
    let receipt = sqlx::query(
        "SELECT disposition, event_type, provider_created_at, retrieval_status, api_version, payload_hash \
         FROM stripe_event_repair_receipts WHERE provider_account_id = $1 AND event_id = $2",
    )
    .bind(&account)
    .bind(&event_id)
    .fetch_one(&pool)
    .await
    .expect("durable API-read receipt exists");
    assert_eq!(
        receipt.try_get::<String, _>("disposition").unwrap(),
        "applied"
    );
    assert_eq!(
        receipt.try_get::<String, _>("event_type").unwrap(),
        "invoice.paid"
    );
    assert_eq!(
        receipt.try_get::<i64, _>("provider_created_at").unwrap(),
        created
    );
    assert_eq!(
        receipt.try_get::<String, _>("retrieval_status").unwrap(),
        "retrieved"
    );
    assert_eq!(
        receipt.try_get::<String, _>("api_version").unwrap(),
        STRIPE_API_VERSION
    );
    assert_eq!(
        receipt.try_get::<String, _>("payload_hash").unwrap().len(),
        64
    );
    let status = status_snapshot(&pool, &account, ProviderEnvironment::Test)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.completed_through, Some(NOW - 29 * 24 * 60 * 60));
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn malformed_event_detail_becomes_a_durable_unresolved_receipt() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let event_id = format!("evt_{}", uuid::Uuid::new_v4().simple());
    let created = NOW - 30 * 24 * 60 * 60 + 200;
    let server = event_api_server(&account, &event_id, created, false, false, true).await;
    let config = stripe_config(&account);
    let stripe = StripeReadClient::for_test(
        "sk_test_event_repair",
        &config,
        server.origin.clone(),
        StripeReadLimits::default(),
    )
    .expect("construct bounded test Stripe client");
    let outcome = run_once(
        &pool,
        &stripe,
        &account,
        ProviderEnvironment::Test,
        NOW,
        |_event, _retrieved_at| async {
            panic!("malformed detail must not enter the event application callback")
        },
    )
    .await
    .expect("persist malformed event as unresolved and continue");
    assert!(matches!(
        outcome,
        StripeEventRepairRunOutcome::PageCompleted {
            events: 1,
            unresolved: 1,
            window_completed: true,
            ..
        }
    ));
    let receipt = sqlx::query(
        "SELECT retrieval_status, api_version, payload_hash, disposition, reason_code \
         FROM stripe_event_repair_receipts WHERE provider_account_id = $1 AND event_id = $2",
    )
    .bind(&account)
    .bind(&event_id)
    .fetch_one(&pool)
    .await
    .expect("unresolved receipt is durable");
    assert_eq!(
        receipt.try_get::<String, _>("retrieval_status").unwrap(),
        "detail_unavailable"
    );
    assert!(receipt
        .try_get::<Option<String>, _>("api_version")
        .unwrap()
        .is_none());
    assert!(receipt
        .try_get::<Option<String>, _>("payload_hash")
        .unwrap()
        .is_none());
    assert_eq!(
        receipt.try_get::<String, _>("disposition").unwrap(),
        "unresolved"
    );
    assert_eq!(
        receipt.try_get::<String, _>("reason_code").unwrap(),
        "event_detail_malformed"
    );
    let status = status_snapshot(&pool, &account, ProviderEnvironment::Test)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.unresolved_count, 1);
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn missing_event_detail_becomes_a_durable_unresolved_receipt() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let event_id = format!("evt_{}", uuid::Uuid::new_v4().simple());
    let created = NOW - 30 * 24 * 60 * 60 + 200;
    let server = event_api_server(&account, &event_id, created, false, true, false).await;
    let config = stripe_config(&account);
    let stripe = StripeReadClient::for_test(
        "sk_test_event_repair",
        &config,
        server.origin.clone(),
        StripeReadLimits::default(),
    )
    .expect("construct bounded test Stripe client");
    let outcome = run_once(
        &pool,
        &stripe,
        &account,
        ProviderEnvironment::Test,
        NOW,
        |_event, _retrieved_at| async {
            panic!("missing detail must not enter the event application callback")
        },
    )
    .await
    .expect("persist missing event detail as unresolved and continue");
    assert!(matches!(
        outcome,
        StripeEventRepairRunOutcome::PageCompleted {
            events: 1,
            unresolved: 1,
            window_completed: true,
            ..
        }
    ));
    let receipt = sqlx::query(
        "SELECT retrieval_status, api_version, payload_hash, disposition, reason_code \
         FROM stripe_event_repair_receipts WHERE provider_account_id = $1 AND event_id = $2",
    )
    .bind(&account)
    .bind(&event_id)
    .fetch_one(&pool)
    .await
    .expect("unresolved receipt is durable");
    assert_eq!(
        receipt.try_get::<String, _>("retrieval_status").unwrap(),
        "detail_unavailable"
    );
    assert!(receipt
        .try_get::<Option<String>, _>("api_version")
        .unwrap()
        .is_none());
    assert!(receipt
        .try_get::<Option<String>, _>("payload_hash")
        .unwrap()
        .is_none());
    assert_eq!(
        receipt.try_get::<String, _>("disposition").unwrap(),
        "unresolved"
    );
    assert_eq!(
        receipt.try_get::<String, _>("reason_code").unwrap(),
        "event_detail_missing"
    );
    let status = status_snapshot(&pool, &account, ProviderEnvironment::Test)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status.unresolved_count, 1);
    assert_eq!(status.completed_through, Some(created - 200 + 24 * 60 * 60));
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn event_repair_resumes_a_page_and_only_completes_after_its_final_receipt() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());

    let first = claim_window(&pool, &account, ProviderEnvironment::Test, NOW)
        .await
        .expect("claim first window")
        .expect("window is available");
    assert_eq!(first.start(), NOW - 30 * 24 * 60 * 60);
    assert_eq!(first.end(), first.start() + 24 * 60 * 60);
    record_event_disposition(
        &pool,
        &first,
        &StripeEventRepairReceipt {
            event_id: "evt_repair_first".into(),
            event_type: "invoice.paid".into(),
            created_at: first.start() + 1,
            retrieved_at: NOW,
            payload: StripeEventRepairPayloadProvenance::Retrieved {
                api_version: "2026-07-29.dahlia".into(),
                payload_hash: "a".repeat(64),
            },
        },
        StripeEventRepairDisposition::Applied,
    )
    .await
    .expect("record first event before moving cursor");
    let changed_disposition = record_event_disposition(
        &pool,
        &first,
        &StripeEventRepairReceipt {
            event_id: "evt_repair_first".into(),
            event_type: "invoice.paid".into(),
            created_at: first.start() + 1,
            retrieved_at: NOW + 1,
            payload: StripeEventRepairPayloadProvenance::Retrieved {
                api_version: "2026-07-29.dahlia".into(),
                payload_hash: "a".repeat(64),
            },
        },
        StripeEventRepairDisposition::Unrelated,
    )
    .await;
    assert!(matches!(
        changed_disposition,
        Err(StripeEventRepairError::ReceiptConflict)
    ));
    let conflicting = record_event_disposition(
        &pool,
        &first,
        &StripeEventRepairReceipt {
            event_id: "evt_repair_first".into(),
            event_type: "invoice.paid".into(),
            created_at: first.start() + 1,
            retrieved_at: NOW + 2,
            payload: StripeEventRepairPayloadProvenance::Retrieved {
                api_version: "2026-07-29.dahlia".into(),
                payload_hash: "c".repeat(64),
            },
        },
        StripeEventRepairDisposition::Applied,
    )
    .await;
    assert!(matches!(
        conflicting,
        Err(StripeEventRepairError::ReceiptConflict)
    ));
    finish_page(&pool, &first, true)
        .await
        .expect("release the unfinished window");

    let resumed = claim_window(&pool, &account, ProviderEnvironment::Test, NOW + 1)
        .await
        .expect("resume existing window")
        .expect("window is available after release");
    assert_eq!(resumed.start(), first.start() + 1);
    assert_eq!(resumed.end(), first.end());
    assert_eq!(resumed.cursor(), None);
    record_event_disposition(
        &pool,
        &resumed,
        &StripeEventRepairReceipt {
            event_id: "evt_repair_second".into(),
            event_type: "invoice.payment_failed".into(),
            created_at: resumed.start() + 2,
            retrieved_at: NOW + 1,
            payload: StripeEventRepairPayloadProvenance::Retrieved {
                api_version: "2026-07-29.dahlia".into(),
                payload_hash: "b".repeat(64),
            },
        },
        StripeEventRepairDisposition::Unresolved("renewal_predecessor_missing"),
    )
    .await
    .expect("record unresolved evidence before advancing cursor");
    finish_page(&pool, &resumed, false)
        .await
        .expect("complete only after final event receipt");

    let row = sqlx::query(
        "SELECT completed_through, window_start, window_end, starting_after, \
                history_gap_start, history_gap_end, oldest_unresolved_created \
         FROM stripe_event_reconciliation_state WHERE provider_account_id = $1 \
           AND provider_environment = 'test'",
    )
    .bind(&account)
    .fetch_one(&pool)
    .await
    .expect("read completed checkpoint");
    assert_eq!(
        row.try_get::<i64, _>("completed_through").unwrap(),
        first.end()
    );
    assert!(row
        .try_get::<Option<i64>, _>("window_start")
        .unwrap()
        .is_none());
    assert!(row
        .try_get::<Option<i64>, _>("window_end")
        .unwrap()
        .is_none());
    assert!(row
        .try_get::<Option<String>, _>("starting_after")
        .unwrap()
        .is_none());
    assert_eq!(
        row.try_get::<Option<i64>, _>("history_gap_start").unwrap(),
        Some(0)
    );
    assert_eq!(
        row.try_get::<Option<i64>, _>("history_gap_end").unwrap(),
        Some(first.start() + 1)
    );
    assert_eq!(
        row.try_get::<Option<i64>, _>("oldest_unresolved_created")
            .unwrap(),
        Some(first.start() + 3)
    );
    let receipts: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM stripe_event_repair_receipts WHERE provider_account_id = $1",
    )
    .bind(&account)
    .fetch_one(&pool)
    .await
    .expect("count durable event receipts");
    assert_eq!(receipts, 2);
    let status = status_snapshot(&pool, &account, ProviderEnvironment::Test)
        .await
        .expect("read aggregate status")
        .expect("status exists after first claim");
    assert!(!status.scan_pending);
    assert!(status.history_gap.is_some());
    assert_eq!(status.last_outcome.as_deref(), Some("complete"));
    assert_eq!(status.unresolved_count, 1);
    assert_eq!(status.unsupported_count, 0);
    assert_eq!(status.pending_scope_count, 0);
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn concurrent_event_repair_claims_have_one_lease_owner() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let (left, right) = tokio::join!(
        claim_window(&pool, &account, ProviderEnvironment::Live, NOW),
        claim_window(&pool, &account, ProviderEnvironment::Live, NOW),
    );
    let left = left.expect("first claimant");
    let right = right.expect("second claimant");
    assert_eq!(
        usize::from(left.is_some()) + usize::from(right.is_some()),
        1
    );
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn renewing_active_event_repair_lease_extends_its_deadline() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let window = claim_window(&pool, &account, ProviderEnvironment::Test, NOW)
        .await
        .expect("claim window")
        .expect("window is available");
    sqlx::query(
        "UPDATE stripe_event_reconciliation_state SET \
         lease_expires_at = now() + interval '30 seconds' \
         WHERE provider_account_id = $1 AND provider_environment = 'test'",
    )
    .bind(&account)
    .execute(&pool)
    .await
    .expect("shorten lease for deterministic check");

    renew_window_lease(&pool, &window)
        .await
        .expect("current owner can extend its active lease");
    let extended: bool = sqlx::query_scalar(
        "SELECT lease_expires_at > now() + interval '200 seconds' \
         FROM stripe_event_reconciliation_state \
         WHERE provider_account_id = $1 AND provider_environment = 'test'",
    )
    .bind(&account)
    .fetch_one(&pool)
    .await
    .expect("read renewed lease");
    assert!(extended);
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn expired_provider_history_becomes_a_durable_gap() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(
        "INSERT INTO stripe_event_reconciliation_state \
         (provider_account_id, provider_environment, completed_through) VALUES ($1, 'live', $2)",
    )
    .bind(&account)
    .bind(NOW - 31 * 24 * 60 * 60)
    .execute(&pool)
    .await
    .expect("seed an old checkpoint");

    let window = claim_window(&pool, &account, ProviderEnvironment::Live, NOW)
        .await
        .expect("claim after horizon loss")
        .expect("newest window can be scanned");
    assert_eq!(window.start(), NOW - 30 * 24 * 60 * 60);
    let row = sqlx::query(
        "SELECT history_gap_start, history_gap_end, oldest_unresolved_created \
         FROM stripe_event_reconciliation_state WHERE provider_account_id = $1 \
           AND provider_environment = 'live'",
    )
    .bind(&account)
    .fetch_one(&pool)
    .await
    .expect("read durable gap");
    assert_eq!(
        row.try_get::<Option<i64>, _>("history_gap_start").unwrap(),
        Some(NOW - 31 * 24 * 60 * 60)
    );
    assert_eq!(
        row.try_get::<Option<i64>, _>("history_gap_end").unwrap(),
        Some(NOW - 30 * 24 * 60 * 60)
    );
    assert_eq!(
        row.try_get::<Option<i64>, _>("oldest_unresolved_created")
            .unwrap(),
        None
    );
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn bound_failure_remains_visible_without_advancing_the_watermark() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let window = claim_window(&pool, &account, ProviderEnvironment::Test, NOW)
        .await
        .unwrap()
        .unwrap();
    fail_window(&pool, &window, "stripe_bound_exceeded", "bound_exceeded")
        .await
        .unwrap();
    let status = status_snapshot(&pool, &account, ProviderEnvironment::Test)
        .await
        .unwrap()
        .unwrap();
    assert!(status.scan_pending);
    assert_eq!(status.completed_through, None);
    assert_eq!(status.last_outcome.as_deref(), Some("bound_exceeded"));
    assert_eq!(
        status.last_error_code.as_deref(),
        Some("stripe_bound_exceeded")
    );
    assert_eq!(
        status.oldest_unresolved_created,
        Some(NOW - 30 * 24 * 60 * 60)
    );
    drop(pool);
    database.cleanup().await;
}

#[tokio::test]
async fn event_repair_waits_for_the_webhook_retry_window_to_pass() {
    let Some(database) = DisposableDatabase::create().await else {
        return;
    };
    let pool = database.pool.clone();
    migrate(&pool).await;
    let account = format!("acct_{}", uuid::Uuid::new_v4().simple());
    let safe_through = NOW - 3 * 24 * 60 * 60;
    sqlx::query(
        "INSERT INTO stripe_event_reconciliation_state \
         (provider_account_id, provider_environment, completed_through) \
         VALUES ($1, 'test', $2)",
    )
    .bind(&account)
    .bind(safe_through)
    .execute(&pool)
    .await
    .expect("seed a caught-up scanner");

    assert!(
        claim_window(&pool, &account, ProviderEnvironment::Test, NOW)
            .await
            .expect("do not scan inside the delivery retry window")
            .is_none()
    );
    let delayed = claim_window(&pool, &account, ProviderEnvironment::Test, NOW + 60 * 60)
        .await
        .expect("resume after the safety delay")
        .expect("newly elapsed repair window is available");
    assert_eq!(delayed.start(), safe_through);
    assert_eq!(delayed.end(), safe_through + 60 * 60);
    drop(pool);
    database.cleanup().await;
}
