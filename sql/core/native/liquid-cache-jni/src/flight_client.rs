//! Lightweight Arrow Flight client that speaks the same protocol as
//! `liquid-cache-common::rpc`.  We replicate the prost message types here
//! so that we do not need to depend on `liquid-cache-common` (which pulls
//! in platform-specific transitive deps like io-uring).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{Any, ProstMessageExt};
use arrow_flight::{Action, Ticket};
use bytes::Bytes;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::prelude::*;
use datafusion_proto::bytes::physical_plan_to_bytes;
use futures::TryStreamExt;
use prost::Message;
use tonic::transport::Channel;
use uuid::Uuid;

// ═══════════════════════════════════════════════════════════════════════
// Replicated RPC message types – binary-compatible with
// `liquid_cache_common::rpc`.
// ═══════════════════════════════════════════════════════════════════════

/// Mirrors `RegisterObjectStoreRequest` from rpc.rs
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterObjectStoreRequest {
    #[prost(string, tag = "1")]
    pub url: String,

    #[prost(map = "string, string", tag = "2")]
    pub options: HashMap<String, String>,
}

impl ProstMessageExt for RegisterObjectStoreRequest {
    fn type_url() -> &'static str {
        ""
    }
    fn as_any(&self) -> Any {
        Any {
            type_url: Self::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

/// Mirrors `RegisterPlanRequest` from rpc.rs
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterPlanRequest {
    #[prost(bytes, tag = "1")]
    pub plan: Vec<u8>,

    #[prost(bytes, tag = "2")]
    pub handle: Bytes,
}

impl ProstMessageExt for RegisterPlanRequest {
    fn type_url() -> &'static str {
        ""
    }
    fn as_any(&self) -> Any {
        Any {
            type_url: Self::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

/// Mirrors `FetchResults` from rpc.rs
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(bytes, tag = "1")]
    pub handle: Bytes,

    #[prost(uint32, tag = "2")]
    pub partition: u32,

    #[prost(string, tag = "3")]
    pub traceparent: String,
}

impl FetchResults {
    pub fn into_ticket(self) -> Ticket {
        Ticket {
            ticket: self.as_any().encode_to_vec().into(),
        }
    }
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        ""
    }
    fn as_any(&self) -> Any {
        Any {
            type_url: Self::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Flight client
// ═══════════════════════════════════════════════════════════════════════

pub struct LiquidCacheFlightClient {
    client: FlightServiceClient<Channel>,
}

impl LiquidCacheFlightClient {
    /// Connect to a Liquid Cache server at the given address
    /// (e.g. `http://localhost:15214`).
    pub async fn connect(server_address: &str) -> Result<Self> {
        let addr = if server_address.starts_with("http://") || server_address.starts_with("https://") {
            server_address.to_string()
        } else {
            format!("http://{server_address}")
        };

        let channel = Channel::from_shared(addr)
            .map_err(to_df_err)?
            .connect()
            .await
            .map_err(to_df_err)?;

        let client = FlightServiceClient::new(channel)
            .max_decoding_message_size(1024 * 1024 * 8);

        Ok(Self { client })
    }

    /// Send `RegisterObjectStore` action to the server.
    pub async fn register_object_store(
        &mut self,
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<()> {
        let request = RegisterObjectStoreRequest {
            url: url.to_string(),
            options,
        };
        let action = Action {
            r#type: "RegisterObjectStore".to_string(),
            body: request.as_any().encode_to_vec().into(),
        };
        self.client
            .do_action(tonic::Request::new(action))
            .await
            .map_err(to_df_err)?;
        Ok(())
    }

    /// Register a serialized physical plan with the server and return the
    /// handle (UUID bytes) that can be used to fetch results.
    pub async fn register_plan(
        &mut self,
        plan_bytes: Vec<u8>,
        handle: Uuid,
    ) -> Result<()> {
        let request = RegisterPlanRequest {
            plan: plan_bytes,
            handle: handle.into_bytes().to_vec().into(),
        };
        let action = Action {
            r#type: "RegisterPlan".to_string(),
            body: request.as_any().encode_to_vec().into(),
        };
        self.client
            .do_action(tonic::Request::new(action))
            .await
            .map_err(to_df_err)?;
        Ok(())
    }

    /// Fetch result batches for the given plan handle + partition.
    pub async fn fetch_results(
        &mut self,
        handle: Uuid,
        partition: u32,
    ) -> Result<Vec<RecordBatch>> {
        let fetch = FetchResults {
            handle: handle.into_bytes().to_vec().into(),
            partition,
            traceparent: String::new(),
        };
        let ticket = fetch.into_ticket();

        let response = self
            .client
            .do_get(ticket)
            .await
            .map_err(to_df_err)?;

        let (_md, response_stream, _ext) = response.into_parts();
        let stream =
            FlightRecordBatchStream::new_from_flight_data(response_stream.map_err(|e| e.into()));

        let batches: Vec<RecordBatch> = stream
            .try_collect()
            .await
            .map_err(to_df_err)?;

        Ok(batches)
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Plan construction helpers
// ═══════════════════════════════════════════════════════════════════════

/// Build a DataFusion physical plan for scanning a Parquet file with
/// optional column projection, then serialize it to bytes that the
/// Liquid Cache server can deserialize.
pub async fn build_parquet_scan_plan(
    file_path: &str,
    columns: Option<Vec<String>>,
    batch_size: usize,
) -> Result<(Vec<u8>, Arc<dyn datafusion::physical_plan::ExecutionPlan>)> {
    // Build a minimal SessionContext for plan generation
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config.options_mut().execution.parquet.schema_force_view_types = false;
    config.options_mut().execution.parquet.binary_as_string = true;
    config.options_mut().execution.batch_size = batch_size;

    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    // Register parquet file as a table
    let table_url = ListingTableUrl::parse(file_path)?;
    let file_format = ParquetFormat::default();
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_file_extension(".parquet");

    let table_config = ListingTableConfig::new(table_url)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    let table = ListingTable::try_new(table_config)?;
    ctx.register_table("__scan__", Arc::new(table))?;

    // Build logical plan with optional projection
    let df = if let Some(cols) = columns {
        let col_exprs: Vec<Expr> = cols.iter().map(|c| col(c.as_str())).collect();
        ctx.table("__scan__").await?.select(col_exprs)?
    } else {
        ctx.table("__scan__").await?
    };

    let logical_plan = df.logical_plan().clone();
    let physical_plan = ctx.state().create_physical_plan(&logical_plan).await?;

    let plan_bytes = physical_plan_to_bytes(physical_plan.clone())?.to_vec();
    Ok((plan_bytes, physical_plan))
}

/// Execute a full scan workflow: register object store, build plan,
/// register plan, fetch all partition results.
pub async fn execute_scan(
    client: &mut LiquidCacheFlightClient,
    object_stores: &[(String, HashMap<String, String>)],
    file_path: &str,
    columns: Option<Vec<String>>,
    batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    // 1. Register object stores
    for (url, options) in object_stores {
        client
            .register_object_store(url, options.clone())
            .await?;
    }

    // 2. Build plan
    let (plan_bytes, physical_plan) =
        build_parquet_scan_plan(file_path, columns, batch_size).await?;

    // 3. Register plan with a new UUID handle
    let handle = Uuid::new_v4();
    client.register_plan(plan_bytes, handle).await?;

    // 4. Fetch results for all partitions
    let num_partitions = physical_plan.output_partitioning().partition_count();
    let mut all_batches = Vec::new();
    for partition in 0..num_partitions {
        let batches = client.fetch_results(handle, partition as u32).await?;
        all_batches.extend(batches);
    }

    Ok(all_batches)
}

fn to_df_err<E: std::error::Error + Send + Sync + 'static>(err: E) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
