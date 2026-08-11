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

//! End-to-end test: write real Arrow batches through the public
//! `WriteBuilder` / `TableWrite` / `TableCommit` API on a table with
//! `changelog-producer=input` + `metadata.iceberg.changelog.storage=local`,
//! and verify the companion Iceberg changelog table is produced
//! automatically — with no direct calls to `IcebergChangelogWriter` or
//! `IcebergChangelogCommitCallback` at all, exercising the exact same path a
//! real caller would go through.
//!
//! Adapted from `paimon-rust` commit `e9955a6`
//! (`crates/integration_tests/tests/iceberg_changelog_e2e.rs`), which drove
//! `IcebergChangelogWriter` directly against a warehouse pre-populated by an
//! external `pypaimon_rust` script (`/tmp/paimon-rust-write-test`) — a
//! fixture that doesn't exist in this checkout or environment. This version
//! is self-contained: it builds the Paimon table it needs in-process against
//! an in-memory `FileIO`, so it always runs (no `#[ignore]`), and it goes
//! through the `CommitCallback`-trait auto-wiring
//! (`WriteBuilder::new_commit()`) added in this port instead of calling the
//! writer directly.

use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use paimon::catalog::Identifier;
use paimon::iceberg::IcebergMetadata;
use paimon::io::{FileIO, FileIOBuilder};
use paimon::spec::{DataType, IntType, Schema, TableSchema};
use paimon::table::Table;
use std::sync::Arc;

fn memory_file_io() -> FileIO {
    FileIOBuilder::new("memory").build().unwrap()
}

async fn setup_dirs(file_io: &FileIO, table_path: &str) {
    file_io
        .mkdirs(&format!("{table_path}/snapshot/"))
        .await
        .unwrap();
    file_io
        .mkdirs(&format!("{table_path}/manifest/"))
        .await
        .unwrap();
}

fn int_batch(ids: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int32, false),
        ArrowField::new("value", ArrowDataType::Int32, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .unwrap()
}

/// A primary-key table with `changelog-producer=input` (so every commit
/// produces a Paimon changelog to mirror) and
/// `metadata.iceberg.changelog.storage=local` (so `WriteBuilder::new_commit`
/// auto-attaches `IcebergChangelogCommitCallback`).
fn iceberg_changelog_table(file_io: &FileIO, table_path: &str) -> Table {
    let schema = Schema::builder()
        .column("id", DataType::Int(IntType::new()))
        .column("value", DataType::Int(IntType::new()))
        .primary_key(["id"])
        .option("bucket", "1")
        .option("changelog-producer", "input")
        .option("metadata.iceberg.changelog.storage", "local")
        .build()
        .unwrap();
    Table::new(
        file_io.clone(),
        Identifier::new("default", "orders"),
        table_path.to_string(),
        TableSchema::new(0, &schema),
        None,
    )
}

#[tokio::test]
async fn test_iceberg_changelog_writer_auto_attached_end_to_end() {
    let file_io = memory_file_io();
    let table_path = "memory:/warehouse/default.db/orders";
    let changelog_meta_path = "memory:/warehouse/default.db/orders_changelog/metadata";
    setup_dirs(&file_io, table_path).await;

    let table = iceberg_changelog_table(&file_io, table_path);

    // ── 1. First commit ──────────────────────────────────────────────────
    let wb = table.new_write_builder();
    let mut write = wb.new_write().unwrap();
    write
        .write_arrow_batch(&int_batch(vec![1, 2], vec![10, 20]))
        .await
        .unwrap();
    let messages = write.prepare_commit().await.unwrap();
    assert_eq!(
        messages[0].new_changelog_files.len(),
        1,
        "changelog-producer=input should emit a changelog file on first commit"
    );
    wb.new_commit().commit(messages).await.unwrap();

    let sm = paimon::table::SnapshotManager::new(file_io.clone(), table_path.to_string());
    let snap1 = sm.get_latest_snapshot().await.unwrap().unwrap();
    assert_eq!(snap1.id(), 1);
    assert!(
        snap1.changelog_manifest_list().is_some(),
        "snapshot 1 should have a changelogManifestList — \
         table was created with changelog-producer=input"
    );

    // ── 2. Companion metadata for snapshot 1 exists ─────────────────────────
    let meta_path_1 = format!("{changelog_meta_path}/v1.metadata.json");
    let meta_input_1 = file_io.new_input(&meta_path_1).unwrap();
    assert!(
        meta_input_1.exists().await.unwrap(),
        "v1.metadata.json should have been written automatically to {meta_path_1}"
    );

    let hint_path = format!("{changelog_meta_path}/version-hint.text");
    let hint_input = file_io.new_input(&hint_path).unwrap();
    assert!(hint_input.exists().await.unwrap());
    let hint_bytes = hint_input.read().await.unwrap();
    let hint = std::str::from_utf8(&hint_bytes).unwrap().trim().to_string();
    assert_eq!(hint, "1");

    let meta_bytes = meta_input_1.read().await.unwrap();
    let meta_str = std::str::from_utf8(&meta_bytes).unwrap();
    let metadata = IcebergMetadata::from_json(meta_str).unwrap();

    assert_eq!(metadata.format_version, 2);
    assert_eq!(metadata.current_snapshot_id, 1);
    assert!(!metadata.table_uuid.is_empty());

    let schema = &metadata.schemas[0];
    let field_names: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
    assert!(
        field_names.contains(&"_value_kind"),
        "_value_kind not in changelog schema: {field_names:?}"
    );
    assert!(
        field_names.contains(&"_sequence_number"),
        "_sequence_number not in changelog schema: {field_names:?}"
    );
    assert!(
        field_names.contains(&"id") && field_names.contains(&"value"),
        "base user columns should also be present: {field_names:?}"
    );

    // ── 3. Second commit advances the companion table too ──────────────────
    let mut write2 = wb.new_write().unwrap();
    write2
        .write_arrow_batch(&int_batch(vec![3], vec![30]))
        .await
        .unwrap();
    let messages2 = write2.prepare_commit().await.unwrap();
    wb.new_commit().commit(messages2).await.unwrap();

    let snap2 = sm.get_latest_snapshot().await.unwrap().unwrap();
    assert_eq!(snap2.id(), 2);

    let meta_path_2 = format!("{changelog_meta_path}/v2.metadata.json");
    assert!(
        file_io
            .new_input(&meta_path_2)
            .unwrap()
            .exists()
            .await
            .unwrap(),
        "v2.metadata.json should exist after the second commit"
    );
    let hint_bytes_2 = file_io.new_input(&hint_path).unwrap().read().await.unwrap();
    let hint_2 = std::str::from_utf8(&hint_bytes_2)
        .unwrap()
        .trim()
        .to_string();
    assert_eq!(
        hint_2, "2",
        "version-hint.text should advance to the latest snapshot"
    );

    // No data files were copied — the companion table only ever wrote
    // metadata/manifest/manifest-list files under `_changelog/metadata`;
    // the physical changelog-*.parquet files still live solely under the
    // main table's own bucket directories.
    let changelog_dir_entries = file_io
        .list_status(&format!("{changelog_meta_path}/"))
        .await
        .unwrap();
    assert!(
        changelog_dir_entries
            .iter()
            .all(|status| !status.path.ends_with(".parquet")),
        "companion metadata directory must contain no parquet data files: {:?}",
        changelog_dir_entries
            .iter()
            .map(|s| &s.path)
            .collect::<Vec<_>>()
    );
}
