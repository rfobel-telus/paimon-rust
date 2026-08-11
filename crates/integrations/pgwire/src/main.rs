// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Paimon pgwire server — PoC PostgreSQL wire protocol front-end backed by
//! DataFusion + PaimonTableProvider.
//!
//! Usage:
//!   paimon-pgwire-server --warehouse /path/to/warehouse [--port 5432]
//!
//! Then connect with:
//!   psql -h localhost -p 5432 -U paimon
//!   psql> SELECT * FROM my_table;
//!   psql> INSERT INTO my_table VALUES (1, 'hello');

use std::sync::Arc;

use async_trait::async_trait;
use clap::Parser;
use datafusion::arrow::array::{
    Array, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use datafusion::arrow::datatypes::{DataType as ArrowDataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::DataFrame;
use futures::stream;
use pgwire::api::query::SimpleQueryHandler;
use pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use pgwire::tokio::process_socket;
use paimon::{CatalogFactory, Options};
use paimon_datafusion::SQLContext;
use tokio::net::TcpListener;

/// Map an Arrow DataType to a PostgreSQL wire protocol type.
fn arrow_type_to_pg(dt: &ArrowDataType) -> Type {
    match dt {
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Int8 => Type::INT2,   // pg has no INT1; use SMALLINT
        ArrowDataType::Int16 => Type::INT2,
        ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::Float32 => Type::FLOAT4,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Type::TEXT,
        ArrowDataType::Date32 | ArrowDataType::Date64 => Type::DATE,
        ArrowDataType::Timestamp(_, _) => Type::TIMESTAMP,
        ArrowDataType::Binary | ArrowDataType::LargeBinary => Type::BYTEA,
        // Fall back to TEXT for anything else (Decimal, List, Struct, etc.)
        _ => Type::TEXT,
    }
}

/// Build the field info list (schema description) from a DataFusion schema.
fn schema_to_fields(schema: &datafusion::arrow::datatypes::Schema) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .map(|f| {
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                arrow_type_to_pg(f.data_type()),
                FieldFormat::Text,
            )
        })
        .collect()
}

/// Encode a single Arrow column value at `row_idx` into the DataRowEncoder.
/// Falls back to a string representation for unsupported types.
fn encode_column(
    encoder: &mut DataRowEncoder,
    col: &Arc<dyn Array>,
    row_idx: usize,
) -> PgWireResult<()> {
    if col.is_null(row_idx) {
        return encoder.encode_field(&None::<i32>);
    }

    match col.data_type() {
        ArrowDataType::Boolean => {
            let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int8 => {
            let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
            encoder.encode_field(&(arr.value(row_idx) as i16))?;
        }
        ArrowDataType::Int16 => {
            let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int32 => {
            let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Int64 => {
            let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Float32 => {
            let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Float64 => {
            let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Utf8 => {
            let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::LargeUtf8 => {
            let arr = col
                .as_any()
                .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
                .unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        ArrowDataType::Date32 => {
            // Encode as ISO-8601 string: days since epoch
            let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx);
            let s = format!("{}", days); // pgwire text protocol accepts epoch-days for DATE
            encoder.encode_field(&s.as_str())?;
        }
        ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            // Send microseconds for TIMESTAMP
            let us = arr.value(row_idx) * 1000;
            encoder.encode_field(&us)?;
        }
        ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr = col
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            encoder.encode_field(&arr.value(row_idx))?;
        }
        // Everything else: stringify
        _ => {
            use datafusion::arrow::array::Array;
            let s = format!("{:?}", col.slice(row_idx, 1));
            encoder.encode_field(&s.as_str())?;
        }
    }

    Ok(())
}

/// Convert a DataFusion result (Vec<RecordBatch>) into pgwire DataRow stream.
fn record_batches_to_row_stream(
    batches: Vec<RecordBatch>,
    schema: Arc<Vec<FieldInfo>>,
) -> impl futures::Stream<Item = PgWireResult<DataRow>> {
    let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();

    for batch in &batches {
        let ncols = batch.num_columns();
        let nrows = batch.num_rows();
        let mut encoder = DataRowEncoder::new(schema.clone());

        for row_idx in 0..nrows {
            for col_idx in 0..ncols {
                let col = batch.column(col_idx);
                if let Err(e) = encode_column(&mut encoder, col, row_idx) {
                    rows.push(Err(e));
                    continue;
                }
            }
            rows.push(Ok(encoder.take_row()));
        }
    }

    stream::iter(rows)
}

// ---------------------------------------------------------------------------
// The pgwire query handler
// ---------------------------------------------------------------------------

struct PaimonQueryHandler {
    ctx: Arc<SQLContext>,
}

impl PaimonQueryHandler {
    fn new(ctx: SQLContext) -> Self {
        Self {
            ctx: Arc::new(ctx),
        }
    }

    /// Execute any SQL through the SQLContext and collect results.
    async fn execute(&self, query: &str) -> PgWireResult<Vec<Response>> {
        log::debug!("SQL: {}", query);

        let df: DataFrame = self
            .ctx
            .sql(query)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // Collect schema before consuming the dataframe
        let arrow_schema = df.schema().as_arrow().clone();
        let field_infos: Arc<Vec<FieldInfo>> = Arc::new(schema_to_fields(&arrow_schema));

        let batches: Vec<RecordBatch> = df
            .collect()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

        // Non-SELECT statements (INSERT, CREATE TABLE, etc.) return an empty result set
        // with a single "count" field or nothing; DataFusion returns an affecetd-rows
        // batch.  Distinguish by checking whether the field list is the empty set or
        // looks like an "affected rows" schema.
        let is_dml = arrow_schema.fields().is_empty()
            || (arrow_schema.fields().len() == 1
                && arrow_schema.field(0).name() == "count");

        if is_dml {
            // Extract affected row count if available
            let count: usize = batches
                .first()
                .and_then(|b| b.column(0).as_any().downcast_ref::<Int64Array>())
                .map(|a| a.value(0) as usize)
                .unwrap_or(0);
            return Ok(vec![Response::Execution(
                Tag::new("OK").with_rows(count),
            )]);
        }

        let row_stream = record_batches_to_row_stream(batches, field_infos.clone());
        Ok(vec![Response::Query(QueryResponse::new(
            field_infos,
            row_stream,
        ))])
    }
}

#[async_trait]
impl SimpleQueryHandler for PaimonQueryHandler {
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        self.execute(query).await.map_err(|e| {
            // Surface DataFusion errors to the client as SQL error messages
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "42000".to_owned(),
                e.to_string(),
            )))
        })
    }
}

// ---------------------------------------------------------------------------
// Factory wiring (pgwire requires a factory that produces handlers per connection)
// ---------------------------------------------------------------------------

struct PaimonServerFactory {
    handler: Arc<PaimonQueryHandler>,
}

impl PgWireServerHandlers for PaimonServerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }
    // startup_handler uses the default NoopHandler (no auth) from PgWireServerHandlers
}

// ---------------------------------------------------------------------------
// CLI entry point
// ---------------------------------------------------------------------------

/// Paimon pgwire server — serve a Paimon warehouse over PostgreSQL wire protocol.
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Path to the Paimon warehouse directory (local filesystem catalog).
    #[arg(long, default_value = "/tmp/paimon-warehouse")]
    warehouse: String,

    /// Catalog name to expose.
    #[arg(long, default_value = "paimon")]
    catalog: String,

    /// Default database inside the catalog.
    #[arg(long, default_value = "default")]
    database: String,

    /// TCP port to listen on.
    #[arg(long, default_value_t = 5432)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    // Build a FileSystem Paimon catalog from the warehouse path
    let mut opts = Options::new();
    opts.set("warehouse", &args.warehouse);
    let catalog = CatalogFactory::create(opts)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Paimon catalog: {e}"))?;

    // Wire catalog into DataFusion through the existing PaimonCatalogProvider
    let mut ctx = SQLContext::new();
    ctx.register_catalog(&args.catalog, catalog)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to register catalog: {e}"))?;

    // Set the default catalog and database so unqualified table names work
    ctx.set_current_catalog(&args.catalog)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set current catalog: {e}"))?;
    ctx.set_current_database(&args.database)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to set current database: {e}"))?;

    let handler = Arc::new(PaimonQueryHandler::new(ctx));
    let factory = Arc::new(PaimonServerFactory { handler });

    let addr = format!("127.0.0.1:{}", args.port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Paimon pgwire server listening on {addr}");
    println!("  warehouse : {}", args.warehouse);
    println!("  catalog   : {}", args.catalog);
    println!("  database  : {}", args.database);
    println!();
    println!("Connect with: psql -h localhost -p {} -U paimon", args.port);

    loop {
        let (socket, peer) = listener.accept().await?;
        log::info!("Accepted connection from {peer}");
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory_ref).await {
                log::warn!("Connection error from {peer}: {e}");
            }
        });
    }
}
