//! JNI entry points for the Liquid Cache native bridge.
//!
//! Java class: `org.apache.spark.sql.execution.liquidcache.LiquidCacheNative`

mod flight_client;
mod session;

use std::collections::HashMap;
use std::sync::Arc;

use arrow_ipc::writer::StreamWriter;
use jni::objects::{JClass, JObjectArray, JString};
use jni::sys::{jbyteArray, jint, jlong};
use jni::JNIEnv;

use session::{
    LiquidCacheSession, ScanResult,
    get_result, get_session, remove_result, remove_session, store_result, store_session,
};

// ═══════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Convert a JString to a Rust String, returning Err on failure.
fn jstring_to_string(env: &mut JNIEnv, s: &JString) -> Result<String, String> {
    env.get_string(s)
        .map(|s| s.into())
        .map_err(|e| format!("Failed to read JString: {e}"))
}

/// Throw a Java RuntimeException with the given message and return the
/// provided default value.
fn throw_and_return<T>(env: &mut JNIEnv, msg: &str, default: T) -> T {
    let _ = env.throw_new("java/lang/RuntimeException", msg);
    default
}

/// Read a Java `String[]` into a `Vec<String>`.
fn jobject_array_to_vec(env: &mut JNIEnv, arr: &JObjectArray) -> Result<Vec<String>, String> {
    let len = env
        .get_array_length(arr)
        .map_err(|e| format!("Failed to get array length: {e}"))?;
    let mut result = Vec::with_capacity(len as usize);
    for i in 0..len {
        let obj = env
            .get_object_array_element(arr, i)
            .map_err(|e| format!("Failed to get array element {i}: {e}"))?;
        let jstr = JString::from(obj);
        result.push(jstring_to_string(env, &jstr)?);
    }
    Ok(result)
}

/// Parse `["key1=value1", "key2=value2", ...]` into a `HashMap`.
fn parse_kv_pairs(pairs: &[String]) -> HashMap<String, String> {
    pairs
        .iter()
        .filter_map(|s| {
            let mut parts = s.splitn(2, '=');
            let k = parts.next()?;
            let v = parts.next()?;
            Some((k.to_string(), v.to_string()))
        })
        .collect()
}

/// Serialize a RecordBatch to Arrow IPC stream bytes.
fn batch_to_ipc_bytes(batch: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, String> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())
            .map_err(|e| format!("IPC writer creation failed: {e}"))?;
        writer
            .write(batch)
            .map_err(|e| format!("IPC write failed: {e}"))?;
        writer
            .finish()
            .map_err(|e| format!("IPC finish failed: {e}"))?;
    }
    Ok(buf)
}

// ═══════════════════════════════════════════════════════════════════════
// JNI entry points
// ═══════════════════════════════════════════════════════════════════════

/// Create a new session connected to the given Liquid Cache server.
/// Returns a session handle (i64).
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_createSession(
    mut env: JNIEnv,
    _class: JClass,
    server_address: JString,
) -> jlong {
    let addr = match jstring_to_string(&mut env, &server_address) {
        Ok(a) => a,
        Err(e) => return throw_and_return(&mut env, &e, 0),
    };

    let session = match LiquidCacheSession::new(addr) {
        Ok(s) => s,
        Err(e) => return throw_and_return(&mut env, &e, 0),
    };

    let arc = Arc::new(session);
    // Use the session's own runtime to run the async store operation.
    // We need a temporary handle because we move `arc` into the async block.
    let rt_handle = arc.runtime.handle().clone();
    let handle = rt_handle.block_on(store_session(arc));
    handle
}

/// Register an object store with the server.
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_registerObjectStore(
    mut env: JNIEnv,
    _class: JClass,
    session_handle: jlong,
    url: JString,
    options: JObjectArray,
) {
    let url_str = match jstring_to_string(&mut env, &url) {
        Ok(u) => u,
        Err(e) => { throw_and_return(&mut env, &e, ()); return; }
    };
    let option_pairs = match jobject_array_to_vec(&mut env, &options) {
        Ok(v) => v,
        Err(e) => { throw_and_return(&mut env, &e, ()); return; }
    };
    let opts = parse_kv_pairs(&option_pairs);

    // Get session
    let session = {
        let rt = tokio::runtime::Handle::try_current();
        // We can't know which runtime is active, so we build a small one if needed.
        let tmp_rt;
        let handle = match &rt {
            Ok(h) => h,
            Err(_) => {
                tmp_rt = tokio::runtime::Runtime::new().unwrap();
                tmp_rt.handle()
            }
        };
        match handle.block_on(get_session(session_handle)) {
            Some(s) => s,
            None => {
                throw_and_return(&mut env, "Invalid session handle", ());
                return;
            }
        }
    };

    let result = session.runtime.block_on(async {
        // Register with remote server
        let mut client = session.client.lock().await;
        client.register_object_store(&url_str, opts.clone()).await?;
        // Remember locally for later plan registrations
        session.object_stores.lock().await.push((url_str, opts));
        Ok::<(), datafusion::error::DataFusionError>(())
    });

    if let Err(e) = result {
        throw_and_return(&mut env, &format!("registerObjectStore failed: {e}"), ());
    }
}

/// Register a parquet file as a named table in the session's DataFusion context.
/// (This is a client-side convenience; the actual scan plan is built at executeScan time.)
/// Currently a no-op placeholder – the file path is used directly in executeScan.
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_registerParquet(
    mut env: JNIEnv,
    _class: JClass,
    session_handle: jlong,
    _table_name: JString,
    _file_path: JString,
) {
    // Validate session exists
    let session = {
        let tmp_rt = tokio::runtime::Runtime::new().unwrap();
        match tmp_rt.block_on(get_session(session_handle)) {
            Some(s) => s,
            None => {
                throw_and_return(&mut env, "Invalid session handle", ());
                return;
            }
        }
    };

    // Parquet registration is deferred to executeScan.
    // We could store the table_name -> file_path mapping here if needed.
    let _ = session;
}

/// Execute a scan: build a physical plan for the given parquet table,
/// register it with the server, fetch all result batches.
/// Returns a result handle (i64).
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_executeScan(
    mut env: JNIEnv,
    _class: JClass,
    session_handle: jlong,
    table_name: JString,  // used as the file path for now
    columns: JObjectArray,
    batch_size: jint,
) -> jlong {
    let file_path = match jstring_to_string(&mut env, &table_name) {
        Ok(p) => p,
        Err(e) => return throw_and_return(&mut env, &e, 0),
    };

    let col_names = match jobject_array_to_vec(&mut env, &columns) {
        Ok(v) => v,
        Err(e) => return throw_and_return(&mut env, &e, 0),
    };

    let cols = if col_names.is_empty() {
        None
    } else {
        Some(col_names)
    };

    let session = {
        let tmp_rt = tokio::runtime::Runtime::new().unwrap();
        match tmp_rt.block_on(get_session(session_handle)) {
            Some(s) => s,
            None => return throw_and_return(&mut env, "Invalid session handle", 0),
        }
    };

    let result = session.runtime.block_on(async {
        let object_stores = session.object_stores.lock().await.clone();
        let mut client = session.client.lock().await;
        flight_client::execute_scan(
            &mut client,
            &object_stores,
            &file_path,
            cols,
            batch_size as usize,
        )
        .await
    });

    match result {
        Ok(batches) => {
            let scan_result = Arc::new(ScanResult::new(batches));
            let tmp_rt = tokio::runtime::Runtime::new().unwrap();
            tmp_rt.block_on(store_result(scan_result))
        }
        Err(e) => throw_and_return(&mut env, &format!("executeScan failed: {e}"), 0),
    }
}

/// Fetch the next Arrow IPC batch from a result handle.
/// Returns the IPC bytes, or null when all batches are consumed.
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_fetchNextBatch(
    mut env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) -> jbyteArray {
    let result = {
        let tmp_rt = tokio::runtime::Runtime::new().unwrap();
        match tmp_rt.block_on(get_result(result_handle)) {
            Some(r) => r,
            None => return throw_and_return(&mut env, "Invalid result handle", std::ptr::null_mut()),
        }
    };

    match result.next_batch() {
        Some(batch) => {
            let ipc_bytes = match batch_to_ipc_bytes(batch) {
                Ok(b) => b,
                Err(e) => return throw_and_return(&mut env, &e, std::ptr::null_mut()),
            };
            match env.byte_array_from_slice(&ipc_bytes) {
                Ok(arr) => arr.into_raw(),
                Err(e) => throw_and_return(
                    &mut env,
                    &format!("Failed to create byte array: {e}"),
                    std::ptr::null_mut(),
                ),
            }
        }
        None => {
            // All batches consumed – return null
            std::ptr::null_mut()
        }
    }
}

/// Close a result handle, freeing its batches.
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_closeResult(
    _env: JNIEnv,
    _class: JClass,
    result_handle: jlong,
) {
    let tmp_rt = tokio::runtime::Runtime::new().unwrap();
    let _ = tmp_rt.block_on(remove_result(result_handle));
}

/// Close a session handle, dropping the Flight client and tokio runtime.
#[no_mangle]
pub extern "system" fn Java_org_apache_spark_sql_execution_liquidcache_LiquidCacheNative_closeSession(
    _env: JNIEnv,
    _class: JClass,
    session_handle: jlong,
) {
    let tmp_rt = tokio::runtime::Runtime::new().unwrap();
    let _ = tmp_rt.block_on(remove_session(session_handle));
}
