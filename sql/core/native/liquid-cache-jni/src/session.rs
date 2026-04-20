use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

use crate::flight_client::LiquidCacheFlightClient;

// ── Handle allocation ──────────────────────────────────────────────────

static NEXT_HANDLE: AtomicI64 = AtomicI64::new(1);
static SESSIONS: Lazy<Mutex<HashMap<i64, Arc<LiquidCacheSession>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));
static RESULTS: Lazy<Mutex<HashMap<i64, Arc<ScanResult>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn alloc_handle() -> i64 {
    NEXT_HANDLE.fetch_add(1, Ordering::Relaxed)
}

// ── Session ────────────────────────────────────────────────────────────

#[allow(dead_code)]
pub struct LiquidCacheSession {
    pub server_address: String,
    pub runtime: tokio::runtime::Runtime,
    pub client: Mutex<LiquidCacheFlightClient>,
    /// Registered object stores (url -> options) for forwarding to the server.
    pub object_stores: Mutex<Vec<(String, HashMap<String, String>)>>,
}

impl LiquidCacheSession {
    pub fn new(server_address: String) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("Failed to create tokio runtime: {e}"))?;

        let client = runtime
            .block_on(LiquidCacheFlightClient::connect(&server_address))
            .map_err(|e| format!("Failed to connect to server: {e}"))?;

        Ok(Self {
            server_address,
            runtime,
            client: Mutex::new(client),
            object_stores: Mutex::new(Vec::new()),
        })
    }
}

pub async fn store_session(session: Arc<LiquidCacheSession>) -> i64 {
    let handle = alloc_handle();
    SESSIONS.lock().await.insert(handle, session);
    handle
}

pub async fn get_session(handle: i64) -> Option<Arc<LiquidCacheSession>> {
    SESSIONS.lock().await.get(&handle).cloned()
}

pub async fn remove_session(handle: i64) -> Option<Arc<LiquidCacheSession>> {
    SESSIONS.lock().await.remove(&handle)
}

// ── Scan result ────────────────────────────────────────────────────────

pub struct ScanResult {
    pub batches: Vec<RecordBatch>,
    pub current_index: AtomicUsize,
}

impl ScanResult {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            current_index: AtomicUsize::new(0),
        }
    }

    /// Returns the next batch, or `None` when all batches have been consumed.
    pub fn next_batch(&self) -> Option<&RecordBatch> {
        let idx = self.current_index.fetch_add(1, Ordering::Relaxed);
        self.batches.get(idx)
    }
}

pub async fn store_result(result: Arc<ScanResult>) -> i64 {
    let handle = alloc_handle();
    RESULTS.lock().await.insert(handle, result);
    handle
}

pub async fn get_result(handle: i64) -> Option<Arc<ScanResult>> {
    RESULTS.lock().await.get(&handle).cloned()
}

pub async fn remove_result(handle: i64) -> Option<Arc<ScanResult>> {
    RESULTS.lock().await.remove(&handle)
}
