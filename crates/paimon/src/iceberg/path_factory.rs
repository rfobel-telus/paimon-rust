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
//
// Ported from `paimon-rust` commit `981942d` unchanged.

use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Generates Iceberg metadata file paths, mirroring Java `IcebergPathFactory`.
///
/// File naming conventions:
/// - metadata JSON:   `v{snapshotId}.metadata.json`
/// - manifest list:   `snap-{counter}-{uuid}.avro`
/// - manifest file:   `{uuid}-m{counter}.avro`
///
/// The per-instance UUID and counters reset with each `IcebergPathFactory` — i.e.
/// one factory per commit, matching the Java behaviour.
pub struct IcebergPathFactory {
    metadata_dir: String,
    uuid: String,
    manifest_list_counter: AtomicU64,
    manifest_counter: AtomicU64,
}

impl IcebergPathFactory {
    /// Create a new factory rooted at `metadata_dir`.
    ///
    /// `metadata_dir` is the path to the `metadata/` directory of the companion
    /// Iceberg table, e.g. `gs://bucket/warehouse/db/table_changelog/metadata`.
    pub fn new(metadata_dir: impl Into<String>) -> Self {
        Self {
            metadata_dir: metadata_dir.into(),
            uuid: Uuid::new_v4().to_string(),
            manifest_list_counter: AtomicU64::new(0),
            manifest_counter: AtomicU64::new(0),
        }
    }

    /// Path to the `metadata/` directory (no trailing slash).
    pub fn metadata_dir(&self) -> &str {
        &self.metadata_dir
    }

    /// Path for a versioned metadata JSON file: `<dir>/v{snapshot_id}.metadata.json`.
    pub fn metadata_path(&self, snapshot_id: i64) -> String {
        format!("{}/v{}.metadata.json", self.metadata_dir, snapshot_id)
    }

    /// Path for the `version-hint.text` file.
    pub fn version_hint_path(&self) -> String {
        format!("{}/version-hint.text", self.metadata_dir)
    }

    /// Next manifest-list file path: `<dir>/snap-{counter}-{uuid}.avro`.
    pub fn next_manifest_list_path(&self) -> String {
        let count = self.manifest_list_counter.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}/snap-{}-{}.avro", self.metadata_dir, count, self.uuid)
    }

    /// Next manifest file path: `<dir>/{uuid}-m{counter}.avro`.
    pub fn next_manifest_path(&self) -> String {
        let count = self.manifest_counter.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}/{}-m{}.avro", self.metadata_dir, self.uuid, count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_factory_naming() {
        let f = IcebergPathFactory::new("memory:/db/tbl_changelog/metadata");
        assert_eq!(
            f.metadata_path(7),
            "memory:/db/tbl_changelog/metadata/v7.metadata.json"
        );
        assert_eq!(
            f.version_hint_path(),
            "memory:/db/tbl_changelog/metadata/version-hint.text"
        );
        // manifest list counter starts at 1
        let ml = f.next_manifest_list_path();
        assert!(ml.contains("/snap-1-"), "expected snap-1-: {ml}");
        assert!(ml.ends_with(".avro"));
        // manifest counter starts at 1
        let m = f.next_manifest_path();
        assert!(m.contains("-m1.avro"), "expected -m1.avro: {m}");
    }
}
