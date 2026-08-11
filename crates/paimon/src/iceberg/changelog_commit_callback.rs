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

//! [`CommitCallback`] wrapper around [`IcebergChangelogWriter`].
//!
//! This is the piece that `paimon-rust` commit `981942d` didn't have (it
//! predates the `CommitCallback` trait added in `ff2170d`): that commit wired
//! `IcebergChangelogWriter` inline into `TableCommit::try_commit_once`,
//! gated by a raw `HashMap` string check, and swallowed writer errors via
//! `eprintln!` so they could never fail a commit.
//!
//! This wrapper instead:
//! - implements [`CommitCallback`] so it composes with any other callback via
//!   [`TableCommit::with_commit_callbacks`](crate::table::TableCommit::with_commit_callbacks),
//! - is gated by the typed
//!   [`CoreOptions::try_iceberg_changelog_storage`](crate::spec::CoreOptions::try_iceberg_changelog_storage)
//!   accessor instead of an inline string check, and
//! - propagates writer errors with `?`, matching the trait's documented
//!   fail-fast default (a callback error fails the whole commit) rather than
//!   swallowing them.

use async_trait::async_trait;

use crate::iceberg::write::IcebergChangelogWriter;
use crate::io::FileIO;
use crate::spec::{Snapshot, TableSchema};
use crate::table::commit_callback::CommitCallback;
use crate::Result;

/// Writes a companion Iceberg changelog table after every commit that
/// produces a Paimon changelog (`snapshot.changelog_manifest_list().is_some()`).
///
/// No-ops (returns `Ok(())` without touching storage) when the just-committed
/// snapshot has no changelog — e.g. a commit that didn't touch any existing
/// primary keys under `changelog-producer=lookup`.
///
/// Construct via [`IcebergChangelogCommitCallback::new`] with the table's
/// `FileIO`, location, and (a snapshot of) its schema — mirroring how
/// [`TableCommit`](crate::table::TableCommit) itself captures `CoreOptions`
/// once at construction time rather than re-resolving them from disk on
/// every commit.
pub struct IcebergChangelogCommitCallback {
    file_io: FileIO,
    table_location: String,
    changelog_metadata_dir: String,
    format_version: i32,
    table_schema: TableSchema,
}

impl IcebergChangelogCommitCallback {
    /// Iceberg format version emitted by this callback (2 is universally
    /// supported by Iceberg readers; matches the Java default and the
    /// `981942d` port). Not currently read from
    /// `metadata.iceberg.format-version` — see module docs on
    /// `crate::iceberg::write` for the full list of carried-over
    /// simplifications.
    pub const DEFAULT_FORMAT_VERSION: i32 = 2;

    /// Build a callback for `table_location`, deriving the companion
    /// metadata directory as `<table_location>_changelog/metadata` (matching
    /// the convention documented on [`IcebergChangelogWriter`]).
    pub fn new(
        file_io: FileIO,
        table_location: impl Into<String>,
        table_schema: TableSchema,
    ) -> Self {
        Self::with_format_version(
            file_io,
            table_location,
            table_schema,
            Self::DEFAULT_FORMAT_VERSION,
        )
    }

    /// Like [`Self::new`], but with an explicit Iceberg format version.
    pub fn with_format_version(
        file_io: FileIO,
        table_location: impl Into<String>,
        table_schema: TableSchema,
        format_version: i32,
    ) -> Self {
        let table_location = table_location.into();
        let changelog_metadata_dir = format!("{table_location}_changelog/metadata");
        Self {
            file_io,
            table_location,
            changelog_metadata_dir,
            format_version,
            table_schema,
        }
    }
}

#[async_trait]
impl CommitCallback for IcebergChangelogCommitCallback {
    async fn call(&self, snapshot: &Snapshot) -> Result<()> {
        if snapshot.changelog_manifest_list().is_none() {
            return Ok(());
        }

        // Ensure the companion metadata directory exists. Best-effort would
        // hide a real permissions/storage problem from the caller, so this
        // uses `?` like everything else here.
        self.file_io
            .mkdirs(&format!("{}/", self.changelog_metadata_dir))
            .await?;

        let writer = IcebergChangelogWriter::new(
            self.file_io.clone(),
            self.table_location.clone(),
            self.changelog_metadata_dir.clone(),
            self.format_version,
        );

        // Propagate errors with `?` — do not swallow them. A companion-table
        // write failure fails the whole commit, per the `CommitCallback`
        // trait's documented fail-fast default.
        writer.write(snapshot.id(), &self.table_schema).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::FileIOBuilder;
    use crate::spec::stats::BinaryTableStats;
    use crate::spec::{
        CommitKind, DataFileMeta, DataType, FileKind, IntType, ManifestEntry, ManifestFileMeta,
        ManifestList, Schema,
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

    fn make_data_file_meta(name: &str, external_path: String) -> DataFileMeta {
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
            external_path: Some(external_path),
            first_row_id: None,
            write_cols: None,
        }
    }

    /// Writes a Paimon snapshot with a changelog manifest list directly
    /// (bypassing `TableWrite`/`TableCommit`), then drives the callback the
    /// same way `TableCommit::try_commit_once` would: `call(&snapshot)`
    /// after the snapshot is durably committed.
    async fn commit_snapshot_with_changelog(file_io: &FileIO, table_path: &str, snapshot_id: i64) {
        file_io
            .mkdirs(&format!("{table_path}/manifest/"))
            .await
            .unwrap();

        let entry = ManifestEntry::new(
            FileKind::Add,
            vec![],
            0,
            1,
            make_data_file_meta(
                &format!("changelog-{snapshot_id}.parquet"),
                format!("{table_path}/bucket-0/changelog-{snapshot_id}.parquet"),
            ),
            2,
        );
        let manifest_name = format!("manifest-changelog-{snapshot_id}");
        let manifest_path = format!("{table_path}/manifest/{manifest_name}");
        crate::spec::Manifest::write(file_io, &manifest_path, &[entry])
            .await
            .unwrap();

        let manifest_meta = ManifestFileMeta::new(
            manifest_name.clone(),
            512,
            1,
            0,
            BinaryTableStats::empty(),
            0,
        );
        let changelog_list_name = format!("manifest-list-changelog-{snapshot_id}");
        let changelog_list_path = format!("{table_path}/manifest/{changelog_list_name}");
        ManifestList::write(file_io, &changelog_list_path, &[manifest_meta])
            .await
            .unwrap();

        let snapshot = Snapshot::builder()
            .version(3)
            .id(snapshot_id)
            .schema_id(0)
            .base_manifest_list(format!("manifest-list-base-{snapshot_id}"))
            .delta_manifest_list(format!("manifest-list-delta-{snapshot_id}"))
            .changelog_manifest_list(Some(changelog_list_name))
            .commit_user("test".to_string())
            .commit_identifier(snapshot_id)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1_000_000)
            .build();

        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        sm.commit_snapshot(&snapshot).await.unwrap();
    }

    #[tokio::test]
    async fn test_call_writes_companion_metadata_when_changelog_present() {
        let file_io = mem_io();
        let table_path = "memory:/warehouse/db/cb_test_table";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();

        commit_snapshot_with_changelog(&file_io, table_path, 1).await;

        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = sm.get_snapshot(1).await.unwrap();

        let callback =
            IcebergChangelogCommitCallback::new(file_io.clone(), table_path, make_table_schema());
        callback.call(&snapshot).await.unwrap();

        let meta_path = format!("{table_path}_changelog/metadata/v1.metadata.json");
        let meta_input = file_io.new_input(&meta_path).unwrap();
        assert!(
            meta_input.exists().await.unwrap(),
            "expected companion metadata.json at {meta_path}"
        );

        let hint_path = format!("{table_path}_changelog/metadata/version-hint.text");
        let hint_input = file_io.new_input(&hint_path).unwrap();
        assert!(hint_input.exists().await.unwrap());
    }

    #[tokio::test]
    async fn test_call_is_noop_without_changelog_manifest_list() {
        let file_io = mem_io();
        let table_path = "memory:/warehouse/db/cb_test_no_changelog";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();

        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("manifest-list-base-1".to_string())
            .delta_manifest_list("manifest-list-delta-1".to_string())
            .commit_user("test".to_string())
            .commit_identifier(1)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1_000_000)
            .build();

        let callback =
            IcebergChangelogCommitCallback::new(file_io.clone(), table_path, make_table_schema());
        callback.call(&snapshot).await.unwrap();

        let meta_dir_exists = file_io
            .exists(&format!("{table_path}_changelog/metadata/"))
            .await
            .unwrap();
        assert!(
            !meta_dir_exists,
            "callback must not create the companion directory when there's no changelog"
        );
    }

    #[tokio::test]
    async fn test_call_is_idempotent_across_repeated_invocations() {
        let file_io = mem_io();
        let table_path = "memory:/warehouse/db/cb_test_idempotent";
        file_io
            .mkdirs(&format!("{table_path}/snapshot/"))
            .await
            .unwrap();

        commit_snapshot_with_changelog(&file_io, table_path, 1).await;
        let sm = SnapshotManager::new(file_io.clone(), table_path.to_string());
        let snapshot = sm.get_snapshot(1).await.unwrap();

        let callback =
            IcebergChangelogCommitCallback::new(file_io.clone(), table_path, make_table_schema());
        callback.call(&snapshot).await.unwrap();
        // A second call for the same already-written snapshot must not error
        // (IcebergChangelogWriter::write is itself idempotent).
        callback.call(&snapshot).await.unwrap();
    }
}
