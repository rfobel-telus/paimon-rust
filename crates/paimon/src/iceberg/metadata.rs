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

//! Plain Rust structs for Iceberg `metadata.json`.
//!
//! All fields use the kebab-case JSON names required by the Iceberg spec.
//! Only the subset needed for the changelog write path is implemented;
//! complex deserialization (e.g. reading back arbitrary metadata files) is
//! intentionally out of scope.
//!
//! Ported from `paimon-rust` commit `981942d` (`feat(iceberg): add
//! IcebergChangelogCommitCallback write+read path`), with import paths
//! adjusted for this checkout. See `IcebergChangelogCommitCallback` in
//! `crate::iceberg::changelog_commit_callback` for the `CommitCallback`
//! wiring, which that original commit did not have (it predates the
//! `CommitCallback` trait).

use crate::spec::{DataField, DataType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// IcebergDataField — mirrors Java IcebergDataField
// ─────────────────────────────────────────────────────────────────────────────

/// A single column in an Iceberg schema, including the Iceberg type string.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergDataField {
    pub id: i32,
    pub name: String,
    pub required: bool,
    /// Iceberg type string (e.g. `"int"`, `"long"`, `"string"`) or nested
    /// object for list/map/struct.  We use `serde_json::Value` so that
    /// round-trip through JSON is lossless for complex types.
    #[serde(rename = "type")]
    pub type_val: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl IcebergDataField {
    /// Build an `IcebergDataField` from a Paimon [`DataField`], converting the
    /// Paimon [`DataType`] to an Iceberg type string.
    pub fn from_paimon(field: &DataField) -> Self {
        let type_val = paimon_type_to_iceberg(field.data_type(), field.id(), 0);
        Self {
            id: field.id(),
            name: field.name().to_owned(),
            required: !field.data_type().is_nullable(),
            type_val,
            doc: field.description().map(str::to_owned),
        }
    }

    /// Build a simple primitive field (used for `_value_kind` / `_sequence_number`).
    pub fn primitive(id: i32, name: impl Into<String>, type_str: &str, nullable: bool) -> Self {
        Self {
            id,
            name: name.into(),
            required: !nullable,
            type_val: serde_json::Value::String(type_str.to_owned()),
            doc: None,
        }
    }

    pub fn with_doc(mut self, doc: impl Into<String>) -> Self {
        self.doc = Some(doc.into());
        self
    }
}

/// Convert a Paimon [`DataType`] to an Iceberg type representation.
///
/// Primitive types → JSON string.  Complex types → JSON object (represented as
/// `serde_json::Value`).  Mirrors Java `IcebergDataField.toTypeObject`.
fn paimon_type_to_iceberg(dt: &DataType, field_id: i32, depth: usize) -> serde_json::Value {
    use serde_json::{json, Value};
    match dt {
        DataType::Boolean(_) => Value::String("boolean".into()),
        DataType::TinyInt(_) | DataType::SmallInt(_) | DataType::Int(_) => {
            Value::String("int".into())
        }
        DataType::BigInt(_) => Value::String("long".into()),
        DataType::Float(_) => Value::String("float".into()),
        DataType::Double(_) => Value::String("double".into()),
        DataType::Date(_) => Value::String("date".into()),
        DataType::Char(_) | DataType::VarChar(_) => Value::String("string".into()),
        DataType::Binary(_) | DataType::VarBinary(_) | DataType::Blob(_) => {
            Value::String("binary".into())
        }
        DataType::Decimal(d) => Value::String(format!("decimal({}, {})", d.precision(), d.scale())),
        DataType::Timestamp(t) => {
            if t.precision() >= 7 {
                Value::String("timestamp_ns".into())
            } else {
                Value::String("timestamp".into())
            }
        }
        DataType::LocalZonedTimestamp(t) => {
            if t.precision() >= 7 {
                Value::String("timestamptz_ns".into())
            } else {
                Value::String("timestamptz".into())
            }
        }
        DataType::Array(arr) => {
            let elem_id = element_field_id(field_id, depth + 1);
            let elem_type = paimon_type_to_iceberg(arr.element_type(), field_id, depth + 1);
            json!({
                "type": "list",
                "element-id": elem_id,
                "element-required": !arr.element_type().is_nullable(),
                "element": elem_type
            })
        }
        DataType::Map(m) => {
            let key_id = map_key_field_id(field_id, depth + 1);
            let val_id = map_value_field_id(field_id, depth + 1);
            let key_type = paimon_type_to_iceberg(m.key_type(), field_id, depth + 1);
            let val_type = paimon_type_to_iceberg(m.value_type(), field_id, depth + 1);
            json!({
                "type": "map",
                "key-id": key_id,
                "key": key_type,
                "value-id": val_id,
                "value-required": !m.value_type().is_nullable(),
                "value": val_type
            })
        }
        DataType::Row(row) => {
            let fields: Vec<serde_json::Value> = row
                .fields()
                .iter()
                .map(|f| {
                    let iceberg = IcebergDataField::from_paimon(f);
                    serde_json::to_value(iceberg).unwrap_or(serde_json::Value::Null)
                })
                .collect();
            json!({ "type": "struct", "fields": fields })
        }
        // Multiset, Vector — not supported in Iceberg; fall back to binary
        _ => Value::String("binary".into()),
    }
}

/// Mirror of Java `SpecialFields.getArrayElementFieldId(fieldId, depth)`.
fn element_field_id(field_id: i32, depth: usize) -> i32 {
    // Java uses  fieldId * 10 + depth  for array element IDs
    field_id * 10 + depth as i32
}

fn map_key_field_id(field_id: i32, depth: usize) -> i32 {
    field_id * 10 + depth as i32 * 2 - 1
}

fn map_value_field_id(field_id: i32, depth: usize) -> i32 {
    field_id * 10 + depth as i32 * 2
}

// ─────────────────────────────────────────────────────────────────────────────
// IcebergSchema
// ─────────────────────────────────────────────────────────────────────────────

/// Iceberg schema — maps to the `schemas` array element in `metadata.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSchema {
    #[serde(rename = "type")]
    pub type_val: String,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    pub fields: Vec<IcebergDataField>,
}

impl IcebergSchema {
    /// Build a schema from a list of Paimon `DataField`s.
    pub fn from_fields(schema_id: i32, fields: Vec<IcebergDataField>) -> Self {
        Self {
            type_val: "struct".to_owned(),
            schema_id,
            fields,
        }
    }

    /// Build a base Iceberg schema from a Paimon `TableSchema` (no extra fields).
    pub fn from_paimon_fields(schema_id: i32, paimon_fields: &[DataField]) -> Self {
        let fields = paimon_fields
            .iter()
            .map(IcebergDataField::from_paimon)
            .collect();
        Self::from_fields(schema_id, fields)
    }

    /// Highest field ID across all direct fields.
    pub fn highest_field_id(&self) -> i32 {
        self.fields.iter().map(|f| f.id).max().unwrap_or(0)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IcebergPartitionField / IcebergPartitionSpec
// ─────────────────────────────────────────────────────────────────────────────

/// `FIRST_FIELD_ID` for partition fields — matches Java constant.
pub const PARTITION_FIRST_FIELD_ID: i32 = 1000;

/// One partition field in the Iceberg partition spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergPartitionField {
    pub name: String,
    pub transform: String,
    #[serde(rename = "source-id")]
    pub source_id: i32,
    #[serde(rename = "field-id")]
    pub field_id: i32,
}

impl IcebergPartitionField {
    pub fn identity(name: impl Into<String>, source_id: i32, field_id: i32) -> Self {
        Self {
            name: name.into(),
            transform: "identity".to_owned(),
            source_id,
            field_id,
        }
    }
}

/// Iceberg partition spec (always spec-id 0 for Paimon).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergPartitionSpec {
    #[serde(rename = "spec-id")]
    pub spec_id: i32,
    pub fields: Vec<IcebergPartitionField>,
}

impl IcebergPartitionSpec {
    pub fn new(fields: Vec<IcebergPartitionField>) -> Self {
        Self { spec_id: 0, fields }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IcebergSortOrder
// ─────────────────────────────────────────────────────────────────────────────

/// Iceberg sort order — always unsorted (order-id=0, empty fields) for Paimon.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSortOrder {
    #[serde(rename = "order-id")]
    pub order_id: i32,
    pub fields: Vec<serde_json::Value>,
}

impl Default for IcebergSortOrder {
    fn default() -> Self {
        Self {
            order_id: 0,
            fields: vec![],
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// IcebergSnapshotSummary / IcebergSnapshot
// ─────────────────────────────────────────────────────────────────────────────

/// Summary map for an Iceberg snapshot (serialised as a plain JSON object).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSnapshotSummary(pub HashMap<String, String>);

impl IcebergSnapshotSummary {
    pub fn append() -> Self {
        let mut m = HashMap::new();
        m.insert("operation".into(), "append".into());
        Self(m)
    }
}

/// One snapshot entry in `metadata.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IcebergSnapshot {
    #[serde(rename = "sequence-number")]
    pub sequence_number: i64,
    #[serde(rename = "snapshot-id")]
    pub snapshot_id: i64,
    #[serde(rename = "parent-snapshot-id", skip_serializing_if = "Option::is_none")]
    pub parent_snapshot_id: Option<i64>,
    #[serde(rename = "timestamp-ms")]
    pub timestamp_ms: u64,
    pub summary: HashMap<String, String>,
    #[serde(rename = "manifest-list")]
    pub manifest_list: String,
    #[serde(rename = "schema-id")]
    pub schema_id: i32,
    #[serde(rename = "first-row-id", skip_serializing_if = "Option::is_none")]
    pub first_row_id: Option<i64>,
    #[serde(rename = "added-rows", skip_serializing_if = "Option::is_none")]
    pub added_rows: Option<i64>,
}

// ─────────────────────────────────────────────────────────────────────────────
// IcebergMetadata — the top-level metadata.json
// ─────────────────────────────────────────────────────────────────────────────

/// Top-level Iceberg `metadata.json` document.
///
/// Supports format-version 2 (default for Paimon Iceberg compat).
/// Only fields required for the changelog read path are included; unknown
/// fields are accepted on deserialization via `#[serde(flatten)]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergMetadata {
    #[serde(rename = "format-version")]
    pub format_version: i32,
    #[serde(rename = "table-uuid")]
    pub table_uuid: String,
    pub location: String,
    #[serde(rename = "last-sequence-number")]
    pub last_sequence_number: i64,
    #[serde(rename = "last-updated-ms")]
    pub last_updated_ms: u64,
    #[serde(rename = "last-column-id")]
    pub last_column_id: i32,
    pub schemas: Vec<IcebergSchema>,
    #[serde(rename = "current-schema-id")]
    pub current_schema_id: i32,
    #[serde(rename = "partition-specs")]
    pub partition_specs: Vec<IcebergPartitionSpec>,
    #[serde(rename = "default-spec-id")]
    pub default_spec_id: i32,
    #[serde(rename = "last-partition-id")]
    pub last_partition_id: i32,
    #[serde(rename = "sort-orders")]
    pub sort_orders: Vec<IcebergSortOrder>,
    #[serde(rename = "default-sort-order-id")]
    pub default_sort_order_id: i32,
    pub snapshots: Vec<IcebergSnapshot>,
    #[serde(rename = "current-snapshot-id")]
    pub current_snapshot_id: i64,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub properties: HashMap<String, String>,
}

impl IcebergMetadata {
    /// Build a `metadata.json` for the changelog companion table.
    ///
    /// - `format_version` — 2 or 3 (from `metadata.iceberg.format-version`)
    /// - `table_uuid`     — stable UUID read from prior version-hint or freshly generated
    /// - `table_location` — path to the _main_ Paimon table (not the _changelog directory)
    /// - `snapshot_id`    — Paimon snapshot ID (used as Iceberg sequence number too)
    /// - `base_schema`    — Iceberg schema built from Paimon user columns only
    /// - `changelog_schema` — `base_schema` + `_value_kind` + `_sequence_number`
    /// - `partition_fields` — mapped from Paimon partition keys
    /// - `manifest_list_path` — full path to the snap-*.avro file just written
    #[allow(clippy::too_many_arguments)]
    pub fn new_changelog(
        format_version: i32,
        table_uuid: String,
        table_location: String,
        snapshot_id: i64,
        base_schema: &IcebergSchema,
        changelog_schema: IcebergSchema,
        partition_fields: Vec<IcebergPartitionField>,
        manifest_list_path: String,
        timestamp_ms: u64,
    ) -> Self {
        let current_schema_id = base_schema.schema_id;
        let last_column_id = changelog_schema.highest_field_id();
        let last_partition_id = partition_fields
            .iter()
            .map(|f| f.field_id)
            .max()
            .unwrap_or(PARTITION_FIRST_FIELD_ID - 1);

        let snapshot = IcebergSnapshot {
            sequence_number: snapshot_id,
            snapshot_id,
            parent_snapshot_id: None,
            timestamp_ms,
            summary: {
                let mut m = HashMap::new();
                m.insert("operation".into(), "append".into());
                m
            },
            manifest_list: manifest_list_path,
            schema_id: current_schema_id,
            first_row_id: None,
            added_rows: None,
        };

        // Paimon writes changelog Parquet files without embedded Iceberg field-id
        // metadata.  Iceberg's reader (both vectorized and non-vectorized) uses
        // field-id lookup to resolve columns.  When field-ids are absent the
        // lookup fails with "Missing required field".
        //
        // The standard Iceberg fix is `schema.name-mapping.default`: a JSON array
        // that maps Parquet column names → field IDs.  With this property set,
        // Iceberg falls back to name-based resolution instead of field-id lookup.
        //
        // Format: [{"field-id": N, "names": ["col", "COL", ...]}, ...]
        //
        // Paimon writes the two metadata columns in UPPER_CASE in the Parquet
        // file (`_SEQUENCE_NUMBER`, `_VALUE_KIND`) even though the Iceberg
        // schema uses lower_case names.  Include the upper-case alias so that
        // Iceberg's name-mapping resolver can match either casing.
        //
        // NOTE (fidelity gap carried over from the ported implementation): the
        // Java reference (`IcebergChangelogCommitCallback`) instead names the
        // Iceberg columns `_VALUE_KIND` / `_SEQUENCE_NUMBER` directly (matching
        // Paimon's physical column names exactly), since it observed that
        // Iceberg falls back to case-sensitive name matching for columns with
        // no embedded field-id. The name-mapping approach here is a legitimate,
        // spec-sanctioned alternative, but it depends on the reading engine
        // honoring `schema.name-mapping.default` (Spark/Trino: yes; some
        // lighter-weight readers may not).
        let name_mapping_json = {
            let entries: Vec<String> = changelog_schema
                .fields
                .iter()
                .map(|f| {
                    let upper = f.name.to_uppercase();
                    if upper != f.name {
                        // Include both lower and upper aliases
                        format!(
                            "{{\"field-id\":{},\"names\":[\"{}\",\"{}\"]}}",
                            f.id, f.name, upper
                        )
                    } else {
                        format!("{{\"field-id\":{},\"names\":[\"{}\"]}}", f.id, f.name)
                    }
                })
                .collect();
            format!("[{}]", entries.join(","))
        };

        let mut properties = HashMap::new();
        properties.insert("schema.name-mapping.default".into(), name_mapping_json);

        Self {
            format_version,
            table_uuid,
            location: table_location,
            last_sequence_number: snapshot_id,
            last_updated_ms: timestamp_ms,
            last_column_id,
            schemas: vec![changelog_schema],
            current_schema_id,
            partition_specs: vec![IcebergPartitionSpec::new(partition_fields)],
            default_spec_id: 0,
            last_partition_id,
            sort_orders: vec![IcebergSortOrder::default()],
            default_sort_order_id: 0,
            snapshots: vec![snapshot],
            current_snapshot_id: snapshot_id,
            properties,
        }
    }

    /// Serialize to a pretty-printed JSON string.
    pub fn to_json(&self) -> crate::Result<String> {
        serde_json::to_string_pretty(self).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to serialize IcebergMetadata to JSON: {e}"),
            source: None,
        })
    }

    /// Deserialize from a JSON string (used to re-read the table UUID).
    pub fn from_json(s: &str) -> crate::Result<Self> {
        serde_json::from_str(s).map_err(|e| crate::Error::DataInvalid {
            message: format!("failed to deserialize IcebergMetadata from JSON: {e}"),
            source: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_schema_from_paimon_fields_round_trips_json() {
        let fields = vec![
            IcebergDataField::primitive(1, "id", "long", false),
            IcebergDataField::primitive(2, "name", "string", true),
        ];
        let schema = IcebergSchema::from_fields(0, fields);
        assert_eq!(schema.highest_field_id(), 2);
        let json = serde_json::to_string(&schema).unwrap();
        let back: IcebergSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(schema, back);
    }

    #[test]
    fn test_iceberg_metadata_round_trips_json() {
        let base =
            IcebergSchema::from_fields(0, vec![IcebergDataField::primitive(1, "val", "int", true)]);
        let changelog = IcebergSchema::from_fields(
            0,
            vec![
                IcebergDataField::primitive(1, "val", "int", true),
                IcebergDataField::primitive(2, "_value_kind", "int", true),
                IcebergDataField::primitive(3, "_sequence_number", "long", true),
            ],
        );
        let meta = IcebergMetadata::new_changelog(
            2,
            "test-uuid".into(),
            "file:///tmp/tbl".into(),
            1,
            &base,
            changelog,
            vec![],
            "file:///tmp/tbl_changelog/metadata/snap-1-abc.avro".into(),
            0,
        );
        let json = meta.to_json().unwrap();
        let back: IcebergMetadata = IcebergMetadata::from_json(&json).unwrap();
        assert_eq!(back.table_uuid, "test-uuid");
        assert_eq!(back.format_version, 2);
        assert_eq!(back.current_snapshot_id, 1);
    }
}
