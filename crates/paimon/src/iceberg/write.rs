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

//! Iceberg manifest and metadata writer for Paimon changelog files.
//!
//! Ports Java `IcebergChangelogCommitCallback.createChangelogMetadata` to Rust.
//! The write path produces:
//!
//! ```text
//! <table>_changelog/metadata/
//!   v{snapshotId}.metadata.json      ← Iceberg metadata doc
//!   version-hint.text                ← pointer to latest snapshot
//!   snap-{N}-{uuid}.avro             ← Iceberg manifest list (Avro)
//!   {uuid}-m{N}.avro                 ← Iceberg manifest file(s) (Avro)
//! ```
//!
//! No data is copied — all paths in the manifest point at the existing
//! `changelog-*.parquet` files that Paimon writes to its own bucket directories.
//!
//! Ported from `paimon-rust` commit `981942d` (`feat(iceberg): add
//! IcebergChangelogCommitCallback write+read path`) unchanged apart from
//! import-path adjustments for this checkout. [`IcebergChangelogWriter`] is
//! the reusable core; the `CommitCallback`-trait wiring that commit didn't
//! have (it predates the trait) now lives in
//! `crate::iceberg::changelog_commit_callback::IcebergChangelogCommitCallback`.
//!
//! Known limitations carried over unchanged from the ported implementation:
//! - **Unpartitioned tables only** — [`IcebergPartitionStruct`] is always an
//!   empty Avro record and `collect_entries` doesn't prepend a partition path
//!   segment when resolving file paths. Partitioned Paimon tables will
//!   produce structurally-valid but partition-blind companion metadata.
//! - Rebuilds the manifest by rescanning **all** retained Paimon snapshots on
//!   every commit rather than keeping Java's incremental Iceberg-side
//!   history. This avoids Java's "unsound full rescan of persisted history"
//!   failure mode (there's no persisted incremental history to corrupt) but
//!   inherits a narrower race: a snapshot Paimon's own expiration is about to
//!   remove (which runs *after* this commit callback, per the `CommitCallback`
//!   contract) can still be visible to this rescan. Unlike Java's design,
//!   this self-heals on the very next commit rather than persisting a
//!   dangling reference indefinitely, since there's no incremental state to
//!   carry the stale entry forward.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::iceberg::metadata::{
    IcebergDataField, IcebergMetadata, IcebergPartitionField, IcebergSchema,
    PARTITION_FIRST_FIELD_ID,
};
use crate::iceberg::path_factory::IcebergPathFactory;
use crate::io::FileIO;
use crate::spec::{FileKind, Manifest, ManifestList, TableSchema};
use crate::table::SnapshotManager;
use crate::Result;

// ─────────────────────────────────────────────────────────────────────────────
// Iceberg Avro manifest wire types
// ─────────────────────────────────────────────────────────────────────────────

/// One row in an Iceberg manifest file.
///
/// Serialised to the Avro schema defined in `ICEBERG_MANIFEST_ENTRY_SCHEMA`.
/// Field IDs match the Iceberg spec (status=0, snapshot_id=1, sequence_number=3,
/// file_sequence_number=4, data_file=2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergManifestEntry {
    /// 0=EXISTING, 1=ADDED, 2=DELETED
    #[serde(rename = "status")]
    pub status: i32,
    #[serde(rename = "snapshot_id")]
    pub snapshot_id: Option<i64>,
    #[serde(rename = "sequence_number")]
    pub sequence_number: Option<i64>,
    #[serde(rename = "file_sequence_number")]
    pub file_sequence_number: Option<i64>,
    #[serde(rename = "data_file")]
    pub data_file: IcebergDataFileMeta,
}

/// Partition struct for Iceberg manifests.
///
/// Iceberg requires the partition to be an Avro **record** (not raw bytes)
/// whose schema matches the partition spec.  For an unpartitioned table the
/// spec has no fields, so this is always an empty record that serialises to
/// `{}` in Avro.  Partitioned tables are not yet supported by this writer.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IcebergPartitionStruct {}

/// One data file record nested inside a manifest entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergDataFileMeta {
    /// 0=DATA, 1=POSITION_DELETES, 2=EQUALITY_DELETES
    #[serde(rename = "content")]
    pub content: i32,
    #[serde(rename = "file_path")]
    pub file_path: String,
    #[serde(rename = "file_format")]
    pub file_format: String,
    /// Partition struct — empty record for unpartitioned tables.
    ///
    /// Iceberg mandates an Avro record type here (not bytes) so that the
    /// Java reader can deserialise it into a `PartitionData` object.
    #[serde(rename = "partition")]
    pub partition: IcebergPartitionStruct,
    #[serde(rename = "record_count")]
    pub record_count: i64,
    #[serde(rename = "file_size_in_bytes")]
    pub file_size_in_bytes: i64,
    #[serde(rename = "null_value_counts")]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    #[serde(rename = "lower_bounds")]
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    #[serde(rename = "upper_bounds")]
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    #[serde(rename = "referenced_data_file")]
    pub referenced_data_file: Option<String>,
    #[serde(rename = "content_offset")]
    pub content_offset: Option<i64>,
    #[serde(rename = "content_size_in_bytes")]
    pub content_size_in_bytes: Option<i64>,
}

/// Summary of a single manifest file — recorded in the manifest list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergManifestFileMeta {
    #[serde(rename = "manifest_path")]
    pub manifest_path: String,
    #[serde(rename = "manifest_length")]
    pub manifest_length: i64,
    #[serde(rename = "partition_spec_id")]
    pub partition_spec_id: i32,
    #[serde(rename = "content")]
    pub content: i32,
    #[serde(rename = "sequence_number")]
    pub sequence_number: i64,
    #[serde(rename = "min_sequence_number")]
    pub min_sequence_number: i64,
    #[serde(rename = "added_snapshot_id")]
    pub added_snapshot_id: Option<i64>,
    #[serde(rename = "added_files_count")]
    pub added_files_count: i32,
    #[serde(rename = "existing_files_count")]
    pub existing_files_count: i32,
    #[serde(rename = "deleted_files_count")]
    pub deleted_files_count: i32,
    #[serde(rename = "added_rows_count")]
    pub added_rows_count: i64,
    #[serde(rename = "existing_rows_count")]
    pub existing_rows_count: i64,
    #[serde(rename = "deleted_rows_count")]
    pub deleted_rows_count: i64,
    #[serde(rename = "partitions")]
    pub partitions: Option<Vec<IcebergPartitionSummary>>,
}

/// Per-partition summary nested in the manifest list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergPartitionSummary {
    #[serde(rename = "contains_null")]
    pub contains_null: bool,
    #[serde(rename = "contains_nan")]
    pub contains_nan: Option<bool>,
    #[serde(rename = "lower_bound")]
    pub lower_bound: Option<Vec<u8>>,
    #[serde(rename = "upper_bound")]
    pub upper_bound: Option<Vec<u8>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Avro schemas (JSON strings) for the manifest file and list
// ─────────────────────────────────────────────────────────────────────────────

/// Avro schema for manifest list entries (`IcebergManifestFileMeta`).
///
/// Mirrors the Iceberg spec manifest list schema with the non-legacy field
/// names (Iceberg 2.x).  The field IDs in metadata comments are informational;
/// the wire format uses index-based ordering.
const ICEBERG_MANIFEST_LIST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_file",
  "namespace": "org.apache.paimon.avro.generated",
  "fields": [
    {"name": "manifest_path",         "type": "string",          "field-id": 500},
    {"name": "manifest_length",       "type": "long",            "field-id": 501},
    {"name": "partition_spec_id",     "type": "int",             "field-id": 502},
    {"name": "content",               "type": "int",             "field-id": 517},
    {"name": "sequence_number",       "type": "long",            "field-id": 515},
    {"name": "min_sequence_number",   "type": "long",            "field-id": 516},
    {"name": "added_snapshot_id",     "type": ["null", "long"],  "default": null, "field-id": 503},
    {"name": "added_files_count",     "type": "int",             "field-id": 504},
    {"name": "existing_files_count",  "type": "int",             "field-id": 505},
    {"name": "deleted_files_count",   "type": "int",             "field-id": 506},
    {"name": "added_rows_count",      "type": "long",            "field-id": 512},
    {"name": "existing_rows_count",   "type": "long",            "field-id": 513},
    {"name": "deleted_rows_count",    "type": "long",            "field-id": 514},
    {"name": "partitions", "type": ["null", {
      "type": "array",
      "items": {
        "type": "record",
        "name": "r508",
        "fields": [
          {"name": "contains_null", "type": "boolean",          "field-id": 509},
          {"name": "contains_nan",  "type": ["null", "boolean"],"default": null, "field-id": 518},
          {"name": "lower_bound",   "type": ["null", "bytes"],  "default": null, "field-id": 510},
          {"name": "upper_bound",   "type": ["null", "bytes"],  "default": null, "field-id": 511}
        ]
      }
    }], "default": null, "field-id": 507}
  ]
}"#;

/// Avro schema for manifest file entries (`IcebergManifestEntry`).
///
/// The `partition` field uses an empty Avro record (`r102`) rather than
/// raw bytes.  The Iceberg Java reader deserialises this into a `PartitionData`
/// object; if it were encoded as `bytes` the reader would throw a
/// `ClassCastException`.  For an unpartitioned table the record has no fields,
/// which serialises as an Avro record with zero fields (wire size = 0 bytes).
const ICEBERG_MANIFEST_ENTRY_SCHEMA: &str = r#"{
  "type": "record",
  "name": "manifest_entry",
  "namespace": "org.apache.paimon.avro.generated",
  "fields": [
    {"name": "status",               "type": "int"},
    {"name": "snapshot_id",          "type": ["null", "long"],   "default": null},
    {"name": "sequence_number",      "type": ["null", "long"],   "default": null},
    {"name": "file_sequence_number", "type": ["null", "long"],   "default": null},
    {"name": "data_file",            "type": {
      "type": "record",
      "name": "r2",
      "fields": [
        {"name": "content",               "type": "int"},
        {"name": "file_path",             "type": "string"},
        {"name": "file_format",           "type": "string"},
        {"name": "partition",             "type": {"type": "record", "name": "r102", "fields": []}},
        {"name": "record_count",          "type": "long"},
        {"name": "file_size_in_bytes",    "type": "long"},
        {"name": "null_value_counts",     "type": ["null", {"type": "map", "values": "long"}], "default": null},
        {"name": "lower_bounds",          "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "upper_bounds",          "type": ["null", {"type": "map", "values": "bytes"}], "default": null},
        {"name": "referenced_data_file",  "type": ["null", "string"], "default": null},
        {"name": "content_offset",        "type": ["null", "long"],   "default": null},
        {"name": "content_size_in_bytes", "type": ["null", "long"],   "default": null}
      ]
    }}
  ]
}"#;

// ─────────────────────────────────────────────────────────────────────────────
// IcebergChangelogWriter
// ─────────────────────────────────────────────────────────────────────────────

/// Writes Iceberg changelog metadata for one Paimon snapshot.
///
/// Call [`IcebergChangelogWriter::write`] after each Paimon commit that has a
/// `changelog_manifest_list`.  The writer:
///
/// 1. Skips if the target `v{id}.metadata.json` already exists (idempotent).
/// 2. Walks all retained snapshots (earliest → latest) collecting ADD entries
///    from each snapshot's changelog manifest list.
/// 3. Writes Avro manifest file(s) + manifest list.
/// 4. Writes `v{id}.metadata.json`.
/// 5. Updates `version-hint.text`.
pub struct IcebergChangelogWriter {
    file_io: FileIO,
    /// Path to the main Paimon table (used as the `location` in metadata.json
    /// and to resolve the manifest directory).
    table_location: String,
    /// Path factory rooted at `<table>_changelog/metadata`.
    path_factory: IcebergPathFactory,
    /// Iceberg format version (2 or 3).
    format_version: i32,
}

impl IcebergChangelogWriter {
    /// Create a writer.
    ///
    /// - `file_io`        — shared `FileIO` for all storage operations.
    /// - `table_location` — path to the Paimon table root (without trailing slash).
    /// - `changelog_metadata_dir` — path to `<table>_changelog/metadata/`.
    /// - `format_version` — Iceberg format version to emit (2 or 3).
    pub fn new(
        file_io: FileIO,
        table_location: impl Into<String>,
        changelog_metadata_dir: impl Into<String>,
        format_version: i32,
    ) -> Self {
        Self {
            file_io,
            table_location: table_location.into(),
            path_factory: IcebergPathFactory::new(changelog_metadata_dir),
            format_version,
        }
    }

    /// Derive the changelog metadata directory from the table location.
    ///
    /// Convenience constructor that mirrors Java:
    /// `<icebergDBPath>/<tableName>_changelog/metadata`.
    ///
    /// `iceberg_db_path` is typically `<table_parent>/.iceberg`.
    pub fn from_table_location(
        file_io: FileIO,
        table_location: impl Into<String>,
        iceberg_db_path: impl AsRef<str>,
        table_name: impl AsRef<str>,
        format_version: i32,
    ) -> Self {
        let changelog_meta = format!(
            "{}/{}_changelog/metadata",
            iceberg_db_path.as_ref(),
            table_name.as_ref()
        );
        Self::new(file_io, table_location, changelog_meta, format_version)
    }

    /// Write Iceberg changelog metadata for the given Paimon snapshot ID.
    ///
    /// Idempotent: if `v{latest_snapshot_id}.metadata.json` already exists,
    /// this is a no-op.  Returns `Ok(true)` when metadata was written,
    /// `Ok(false)` when it was skipped.
    pub async fn write(&self, latest_snapshot_id: i64, table_schema: &TableSchema) -> Result<bool> {
        let target_path = self.path_factory.metadata_path(latest_snapshot_id);

        // Idempotency guard — skip if already written
        let input = self.file_io.new_input(&target_path)?;
        if input.exists().await? {
            return Ok(false);
        }

        let snapshot_manager =
            SnapshotManager::new(self.file_io.clone(), self.table_location.clone());
        let earliest_id = match snapshot_manager.earliest_snapshot_id().await? {
            Some(id) => id,
            None => return Ok(false),
        };

        // Build Iceberg schemas from Paimon table schema
        let base_schema = self.build_base_schema(table_schema);
        let changelog_schema = self.build_changelog_schema(&base_schema, table_schema);
        let partition_fields = self.build_partition_fields(table_schema, &base_schema);

        // Collect all ADD entries from all retained snapshots' changelog manifests
        let manifest_dir = snapshot_manager.manifest_dir();
        let entries = self
            .collect_entries(
                &snapshot_manager,
                earliest_id,
                latest_snapshot_id,
                &manifest_dir,
            )
            .await?;

        // Write manifest file(s) + manifest list
        let manifest_list_path = self
            .write_manifest_list(&entries, latest_snapshot_id)
            .await?;

        // Read or generate a stable table UUID
        let table_uuid = self.get_or_create_table_uuid().await;

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let metadata = IcebergMetadata::new_changelog(
            self.format_version,
            table_uuid,
            self.table_location.clone(),
            latest_snapshot_id,
            &base_schema,
            changelog_schema,
            partition_fields,
            manifest_list_path,
            now_ms,
        );

        // Write metadata.json (best-effort atomic)
        let json = metadata.to_json()?;
        let out = self.file_io.new_output(&target_path)?;
        out.write(bytes::Bytes::from(json.into_bytes())).await?;

        // Update version-hint.text
        let hint_path = self.path_factory.version_hint_path();
        let hint_out = self.file_io.new_output(&hint_path)?;
        hint_out
            .write(bytes::Bytes::from(
                latest_snapshot_id.to_string().into_bytes(),
            ))
            .await?;

        Ok(true)
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Private helpers
    // ─────────────────────────────────────────────────────────────────────────

    /// Build the base Iceberg schema from Paimon user columns.
    fn build_base_schema(&self, table_schema: &TableSchema) -> IcebergSchema {
        IcebergSchema::from_paimon_fields(table_schema.id() as i32, table_schema.fields())
    }

    /// Append `_value_kind` (int) and `_sequence_number` (long) to the base schema.
    fn build_changelog_schema(
        &self,
        base_schema: &IcebergSchema,
        _table_schema: &TableSchema,
    ) -> IcebergSchema {
        let highest = base_schema.highest_field_id();
        let mut fields = base_schema.fields.clone();
        fields.push(
            IcebergDataField::primitive(highest + 1, "_value_kind", "int", true).with_doc(
                "Paimon change type: 0=INSERT, 1=UPDATE_BEFORE, 2=UPDATE_AFTER, 3=DELETE",
            ),
        );
        fields.push(
            IcebergDataField::primitive(highest + 2, "_sequence_number", "long", true)
                .with_doc("Paimon sequence number, monotonically increasing within a checkpoint"),
        );
        IcebergSchema::from_fields(base_schema.schema_id, fields)
    }

    /// Map Paimon partition keys to Iceberg partition fields.
    fn build_partition_fields(
        &self,
        table_schema: &TableSchema,
        iceberg_schema: &IcebergSchema,
    ) -> Vec<IcebergPartitionField> {
        let field_map: HashMap<&str, i32> = iceberg_schema
            .fields
            .iter()
            .map(|f| (f.name.as_str(), f.id))
            .collect();

        let mut result = Vec::new();
        let mut field_id = PARTITION_FIRST_FIELD_ID;
        for pk in table_schema.partition_keys() {
            if let Some(&source_id) = field_map.get(pk.as_str()) {
                result.push(IcebergPartitionField::identity(pk, source_id, field_id));
                field_id += 1;
            }
        }
        result
    }

    /// Walk all retained snapshots and collect ADD manifest entries from their
    /// changelog manifest lists.
    async fn collect_entries(
        &self,
        snapshot_manager: &SnapshotManager,
        earliest_id: i64,
        latest_id: i64,
        manifest_dir: &str,
    ) -> Result<Vec<IcebergManifestEntry>> {
        let mut entries = Vec::new();

        for snapshot_id in earliest_id..=latest_id {
            // It's fine if a snapshot was expired — just skip it
            let snapshot = match snapshot_manager.get_snapshot(snapshot_id).await {
                Ok(s) => s,
                Err(_) => continue,
            };

            let changelog_list = match snapshot.changelog_manifest_list() {
                Some(name) => name.to_owned(),
                None => continue,
            };

            let list_path = format!("{}/{}", manifest_dir, changelog_list);
            let metas = ManifestList::read(&self.file_io, &list_path).await?;

            for meta in &metas {
                let manifest_path = format!("{}/{}", manifest_dir, meta.file_name());
                let manifest_entries = Manifest::read(&self.file_io, &manifest_path).await?;

                for entry in manifest_entries {
                    if entry.kind() != &FileKind::Add {
                        continue;
                    }

                    let file = entry.file();

                    // Resolve the actual file path.
                    // For changelog files, `external_path` is set when the file lives
                    // outside the standard bucket layout (e.g. GCS object path).
                    // Otherwise fall back to <table>/<partition-bucket>/<file_name>.
                    let file_path = if let Some(ext) = &file.external_path {
                        ext.clone()
                    } else {
                        // Build standard bucket path:
                        // <table_location>/bucket-<bucket>/<file_name>
                        format!(
                            "{}/bucket-{}/{}",
                            self.table_location,
                            entry.bucket(),
                            file.file_name
                        )
                    };

                    let iceberg_entry = IcebergManifestEntry {
                        status: 1, // ADDED
                        snapshot_id: Some(snapshot_id),
                        sequence_number: Some(snapshot_id),
                        file_sequence_number: Some(snapshot_id),
                        data_file: IcebergDataFileMeta {
                            content: 0, // DATA
                            file_path,
                            file_format: "parquet".to_owned(),
                            // Iceberg requires an Avro record here (not raw bytes).
                            // For unpartitioned tables this is always an empty struct.
                            partition: IcebergPartitionStruct::default(),
                            record_count: file.row_count,
                            file_size_in_bytes: file.file_size,
                            null_value_counts: None,
                            lower_bounds: None,
                            upper_bounds: None,
                            referenced_data_file: None,
                            content_offset: None,
                            content_size_in_bytes: None,
                        },
                    };
                    entries.push(iceberg_entry);
                }
            }
        }

        Ok(entries)
    }

    /// Write entries to one Avro manifest file, then write a manifest list.
    /// Returns the full path of the manifest list file.
    async fn write_manifest_list(
        &self,
        entries: &[IcebergManifestEntry],
        snapshot_id: i64,
    ) -> Result<String> {
        let manifest_list_path = self.path_factory.next_manifest_list_path();

        if entries.is_empty() {
            // Write an empty manifest list
            let bytes = crate::spec::to_avro_bytes(
                ICEBERG_MANIFEST_LIST_SCHEMA,
                &[] as &[IcebergManifestFileMeta],
            )?;
            let out = self.file_io.new_output(&manifest_list_path)?;
            out.write(bytes::Bytes::from(bytes)).await?;
            return Ok(manifest_list_path);
        }

        // Write one manifest file containing all entries
        let manifest_path = self.path_factory.next_manifest_path();
        let manifest_bytes = crate::spec::to_avro_bytes(ICEBERG_MANIFEST_ENTRY_SCHEMA, entries)?;

        let total_rows: i64 = entries.iter().map(|e| e.data_file.record_count).sum();

        let out = self.file_io.new_output(&manifest_path)?;
        out.write(bytes::Bytes::from(manifest_bytes.clone()))
            .await?;

        let manifest_meta = IcebergManifestFileMeta {
            manifest_path: manifest_path.clone(),
            manifest_length: manifest_bytes.len() as i64,
            partition_spec_id: 0,
            content: 0, // DATA
            sequence_number: snapshot_id,
            min_sequence_number: snapshot_id,
            added_snapshot_id: Some(snapshot_id),
            added_files_count: entries.len() as i32,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: total_rows,
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: None,
        };

        let list_bytes =
            crate::spec::to_avro_bytes(ICEBERG_MANIFEST_LIST_SCHEMA, &[manifest_meta])?;
        let list_out = self.file_io.new_output(&manifest_list_path)?;
        list_out.write(bytes::Bytes::from(list_bytes)).await?;

        Ok(manifest_list_path)
    }

    /// Return the existing table UUID (read from prior version-hint + metadata),
    /// or generate a fresh UUID if no prior metadata exists.
    async fn get_or_create_table_uuid(&self) -> String {
        // Try to read the version-hint to find the last metadata file
        let hint_path = self.path_factory.version_hint_path();
        if let Ok(input) = self.file_io.new_input(&hint_path) {
            if let Ok(true) = input.exists().await {
                if let Ok(bytes) = input.read().await {
                    if let Ok(content) = std::str::from_utf8(&bytes) {
                        if let Ok(version) = content.trim().parse::<i64>() {
                            let meta_path = self.path_factory.metadata_path(version);
                            if let Ok(meta_input) = self.file_io.new_input(&meta_path) {
                                if let Ok(true) = meta_input.exists().await {
                                    if let Ok(meta_bytes) = meta_input.read().await {
                                        if let Ok(s) = std::str::from_utf8(&meta_bytes) {
                                            if let Ok(meta) = IcebergMetadata::from_json(s) {
                                                return meta.table_uuid;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Uuid::new_v4().to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        CommitKind, DataFileMeta, DataType, IntType, ManifestEntry, ManifestFileMeta, ManifestList,
        Schema, Snapshot, TableSchema,
    };
    use crate::table::SnapshotManager;

    fn mem_io() -> FileIO {
        FileIOBuilder::new("memory").build().unwrap()
    }

    fn make_table_schema() -> TableSchema {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::default()))
            .column("val", DataType::Int(IntType::default()))
            .build()
            .unwrap();
        TableSchema::new(0, &schema)
    }

    fn make_data_file_meta(name: &str) -> DataFileMeta {
        DataFileMeta {
            file_name: name.to_string(),
            file_size: 1024,
            row_count: 10,
            min_key: vec![],
            max_key: vec![],
            key_stats: BinaryTableStats::empty(),
            value_stats: BinaryTableStats::empty(),
            min_sequence_number: 0,
            max_sequence_number: 0,
            schema_id: 0,
            level: 0,
            extra_files: vec![],
            creation_time: None,
            delete_row_count: None,
            embedded_index: None,
            file_source: None,
            value_stats_cols: None,
            external_path: Some(format!("memory:/tbl/bucket-0/{name}")),
            first_row_id: None,
            write_cols: None,
        }
    }

    /// Full end-to-end integration test:
    /// 1. Set up Paimon snapshot + changelog manifest in memory FileIO
    /// 2. Call IcebergChangelogWriter::write
    /// 3. Assert metadata.json and version-hint.text exist and are valid
    #[tokio::test]
    async fn test_iceberg_changelog_writer_end_to_end() {
        let file_io = mem_io();
        let table_path = "memory:/warehouse/db/test_table";
        let changelog_meta_path = "memory:/warehouse/db/test_table_changelog/metadata";

        // ── 1. Create directory structure ──────────────────────────────────
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{table_path}/schema/"))
            .await
            .unwrap();
        file_io
            .mkdirs(&format!("{changelog_meta_path}/"))
            .await
            .unwrap();

        // ── 2. Write a Paimon changelog manifest file ──────────────────────
        let entry = ManifestEntry::new(
            FileKind::Add,
            vec![], // no partition bytes for unpartitioned table
            0,      // bucket
            1,      // total buckets
            make_data_file_meta("changelog-0.parquet"),
            2, // version
        );
        let manifest_name = "manifest-changelog-1";
        let manifest_path = format!("{table_path}/manifest/{manifest_name}");
        crate::spec::Manifest::write(&file_io, &manifest_path, &[entry])
            .await
            .unwrap();

        // ── 3. Write Paimon manifest list (changelog) ──────────────────────
        let manifest_meta = ManifestFileMeta::new(
            manifest_name.to_string(),
            512,
            1, // num_added_files
            0, // num_deleted_files
            BinaryTableStats::empty(),
            0, // schema_id
        );
        let changelog_list_name = "manifest-list-changelog-1";
        let changelog_list_path = format!("{table_path}/manifest/{changelog_list_name}");
        ManifestList::write(&file_io, &changelog_list_path, &[manifest_meta])
            .await
            .unwrap();

        // ── 4. Write a Paimon snapshot that references the changelog list ──
        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("manifest-list-base-1".to_string())
            .delta_manifest_list("manifest-list-delta-1".to_string())
            .changelog_manifest_list(Some(changelog_list_name.to_string()))
            .commit_user("test".to_string())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1_000_000)
            .build();

        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        sm.commit_snapshot(&snapshot).await.unwrap();

        // ── 5. Build the table schema ──────────────────────────────────────
        let table_schema = make_table_schema();

        // ── 6. Run the Iceberg changelog writer ────────────────────────────
        let writer = IcebergChangelogWriter::new(
            file_io.clone(),
            table_path,
            changelog_meta_path,
            2, // format_version
        );

        let written = writer.write(1, &table_schema).await.unwrap();
        assert!(written, "expected metadata to be written on first call");

        // ── 7. Verify metadata.json exists and parses correctly ───────────
        let meta_path = format!("{changelog_meta_path}/v1.metadata.json");
        let meta_input = file_io.new_input(&meta_path).unwrap();
        assert!(
            meta_input.exists().await.unwrap(),
            "v1.metadata.json should exist"
        );
        let meta_bytes = meta_input.read().await.unwrap();
        let meta_str = std::str::from_utf8(&meta_bytes).unwrap();
        let metadata: IcebergMetadata = IcebergMetadata::from_json(meta_str).unwrap();

        assert_eq!(metadata.format_version, 2);
        assert_eq!(metadata.current_snapshot_id, 1);
        assert!(!metadata.table_uuid.is_empty());
        // changelog schema has base columns + _value_kind + _sequence_number
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

        // ── 8. Verify version-hint.text ────────────────────────────────────
        let hint_path = format!("{changelog_meta_path}/version-hint.text");
        let hint_input = file_io.new_input(&hint_path).unwrap();
        assert!(
            hint_input.exists().await.unwrap(),
            "version-hint.text should exist"
        );
        let hint_bytes = hint_input.read().await.unwrap();
        let hint = std::str::from_utf8(&hint_bytes).unwrap().trim().to_string();
        assert_eq!(hint, "1", "version-hint.text should contain snapshot id 1");

        // ── 9. Idempotency: second call should return false ─────────────────
        let written2 = writer.write(1, &table_schema).await.unwrap();
        assert!(
            !written2,
            "second write for same snapshot should be skipped (idempotent)"
        );
    }

    #[test]
    fn test_iceberg_manifest_entry_avro_roundtrip() {
        let entry = IcebergManifestEntry {
            status: 1,
            snapshot_id: Some(42),
            sequence_number: Some(42),
            file_sequence_number: Some(42),
            data_file: IcebergDataFileMeta {
                content: 0,
                file_path: "memory:/tbl/bucket-0/changelog-0.parquet".into(),
                file_format: "parquet".into(),
                partition: IcebergPartitionStruct::default(),
                record_count: 100,
                file_size_in_bytes: 4096,
                null_value_counts: None,
                lower_bounds: None,
                upper_bounds: None,
                referenced_data_file: None,
                content_offset: None,
                content_size_in_bytes: None,
            },
        };

        let bytes = crate::spec::to_avro_bytes(ICEBERG_MANIFEST_ENTRY_SCHEMA, &[entry.clone()])
            .expect("serialize manifest entry");
        assert!(!bytes.is_empty(), "avro bytes should not be empty");

        // Deserialise back
        let decoded: Vec<IcebergManifestEntry> =
            crate::spec::from_avro_bytes(&bytes).expect("deserialize manifest entry");
        assert_eq!(decoded.len(), 1);
        let d = &decoded[0];
        assert_eq!(d.status, 1);
        assert_eq!(d.data_file.record_count, 100);
        assert_eq!(
            d.data_file.file_path,
            "memory:/tbl/bucket-0/changelog-0.parquet"
        );
    }

    #[test]
    fn test_iceberg_manifest_list_avro_roundtrip() {
        let meta = IcebergManifestFileMeta {
            manifest_path: "memory:/meta/uuid-m1.avro".into(),
            manifest_length: 1024,
            partition_spec_id: 0,
            content: 0,
            sequence_number: 5,
            min_sequence_number: 5,
            added_snapshot_id: Some(5),
            added_files_count: 3,
            existing_files_count: 0,
            deleted_files_count: 0,
            added_rows_count: 300,
            existing_rows_count: 0,
            deleted_rows_count: 0,
            partitions: None,
        };

        let bytes = crate::spec::to_avro_bytes(ICEBERG_MANIFEST_LIST_SCHEMA, &[meta])
            .expect("serialize manifest list");
        assert!(!bytes.is_empty());

        let decoded: Vec<IcebergManifestFileMeta> =
            crate::spec::from_avro_bytes(&bytes).expect("deserialize manifest list");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].manifest_path, "memory:/meta/uuid-m1.avro");
        assert_eq!(decoded[0].added_files_count, 3);
    }

    /// Filesystem integration test: invoke IcebergChangelogWriter against
    /// the real on-disk Paimon table written by pypaimon_rust.
    ///
    /// Requires the warehouse at /tmp/paimon-rust-write-test/ to have been
    /// pre-populated by running the pypaimon_rust write script (see task spec).
    ///
    /// Run with:
    ///   cargo test -p paimon iceberg::write::tests::test_iceberg_changelog_writer_filesystem -- --nocapture
    #[ignore = "requires pre-populated warehouse at /tmp/paimon-rust-write-test — run pypaimon_rust write script first"]
    #[tokio::test]
    async fn test_iceberg_changelog_writer_filesystem() {
        let warehouse = "/tmp/paimon-rust-write-test";
        let table_path = format!("{warehouse}/default.db/orders");
        let changelog_meta_path = format!("{warehouse}/default.db/orders_changelog/metadata");

        // Skip if the warehouse doesn't exist (CI without pre-populated data)
        if !std::path::Path::new(&table_path).exists() {
            eprintln!(
                "SKIP: warehouse not found at {table_path}; run pypaimon_rust write script first"
            );
            return;
        }

        let file_io = FileIOBuilder::new("file")
            .build()
            .expect("build file FileIO");

        // Load and parse the table schema
        let schema_path = format!("{table_path}/schema/schema-0");
        let schema_input = file_io.new_input(&schema_path).expect("schema input");
        assert!(
            schema_input.exists().await.expect("schema exists check"),
            "schema-0 not found at {schema_path}"
        );
        let schema_bytes = schema_input.read().await.expect("read schema");
        let schema_json = std::str::from_utf8(&schema_bytes).expect("schema utf8");
        let table_schema: TableSchema =
            serde_json::from_str(schema_json).expect("parse TableSchema");
        eprintln!(
            "Loaded schema: fields={:?}",
            table_schema
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        // Verify the latest snapshot has a changelog manifest list
        let sm = SnapshotManager::new(file_io.clone(), table_path.clone());
        let latest = sm
            .get_latest_snapshot()
            .await
            .expect("get_latest_snapshot")
            .expect("at least one snapshot");
        assert!(
            latest.changelog_manifest_list().is_some(),
            "snapshot {} has no changelogManifestList — table must use changelog-producer=lookup",
            latest.id()
        );
        eprintln!(
            "Snapshot {} changelogManifestList: {:?}",
            latest.id(),
            latest.changelog_manifest_list()
        );

        // Create the changelog metadata directory
        file_io
            .mkdirs(&format!("{changelog_meta_path}/"))
            .await
            .expect("mkdirs changelog_meta_path");

        // Run the writer
        let writer = IcebergChangelogWriter::new(
            file_io.clone(),
            table_path.clone(),
            changelog_meta_path.clone(),
            2,
        );
        let written = writer
            .write(latest.id(), &table_schema)
            .await
            .expect("IcebergChangelogWriter::write");
        assert!(written, "expected first write to produce metadata");
        eprintln!(
            "IcebergChangelogWriter produced metadata for snapshot {}",
            latest.id()
        );

        // Verify version-hint.text
        let hint_path = format!("{changelog_meta_path}/version-hint.text");
        let hint_input = file_io.new_input(&hint_path).expect("hint input");
        assert!(
            hint_input.exists().await.expect("hint exists"),
            "version-hint.text missing"
        );
        let hint_bytes = hint_input.read().await.expect("read hint");
        let hint = std::str::from_utf8(&hint_bytes)
            .expect("hint utf8")
            .trim()
            .to_string();
        assert_eq!(hint, latest.id().to_string(), "version-hint.text mismatch");
        eprintln!("version-hint.text = {hint}");

        // Verify metadata.json contains _value_kind and _sequence_number
        let meta_path = format!("{changelog_meta_path}/v{}.metadata.json", latest.id());
        let meta_input = file_io.new_input(&meta_path).expect("meta input");
        assert!(
            meta_input.exists().await.expect("meta exists"),
            "metadata.json missing"
        );
        let meta_bytes = meta_input.read().await.expect("read meta");
        let meta_str = std::str::from_utf8(&meta_bytes).expect("meta utf8");
        let meta: serde_json::Value = serde_json::from_str(meta_str).expect("parse metadata.json");

        assert_eq!(meta["format-version"].as_i64(), Some(2));
        assert_eq!(meta["current-snapshot-id"].as_i64(), Some(latest.id()));

        let schemas = meta["schemas"].as_array().expect("schemas array");
        let fields = schemas[0]["fields"].as_array().expect("fields");
        let field_names: Vec<&str> = fields.iter().filter_map(|f| f["name"].as_str()).collect();
        eprintln!("Iceberg schema fields: {:?}", field_names);
        assert!(
            field_names.contains(&"_value_kind"),
            "_value_kind missing from schema"
        );
        assert!(
            field_names.contains(&"_sequence_number"),
            "_sequence_number missing from schema"
        );

        // Idempotency check
        let written2 = writer
            .write(latest.id(), &table_schema)
            .await
            .expect("second write");
        assert!(!written2, "second write should be idempotent");

        eprintln!("All assertions passed!");
        eprintln!("Iceberg changelog metadata at: {changelog_meta_path}");
        for entry in std::fs::read_dir(&changelog_meta_path)
            .expect("read dir")
            .flatten()
        {
            eprintln!("  {}", entry.file_name().to_string_lossy());
        }
    }
}
