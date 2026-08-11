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

//! Iceberg changelog metadata writer.
//!
//! Ports `IcebergChangelogCommitCallback` from Java to Rust: after each
//! Paimon commit that produces a `changelog_manifest_list`, this module
//! writes a companion Iceberg metadata tree at `<table>_changelog/metadata/`
//! so that changelog parquet files can be read by any Iceberg-compatible
//! engine without data duplication.
//!
//! `metadata`, `path_factory`, and `write` are ported near-verbatim from
//! `paimon-rust` commit `981942d` (a sibling branch built on an older base
//! that predates this fork's `CommitCallback` trait). `changelog_commit_callback`
//! is new: it's the `CommitCallback`-trait wrapper that lets
//! [`IcebergChangelogCommitCallback`] attach via
//! [`TableCommit::with_commit_callbacks`](crate::table::TableCommit::with_commit_callbacks)
//! instead of being wired inline into the commit path.

mod changelog_commit_callback;
mod metadata;
mod path_factory;
mod write;

pub use changelog_commit_callback::IcebergChangelogCommitCallback;
pub use metadata::{
    IcebergDataField, IcebergMetadata, IcebergPartitionField, IcebergPartitionSpec, IcebergSchema,
    IcebergSnapshot, IcebergSnapshotSummary, IcebergSortOrder,
};
pub use path_factory::IcebergPathFactory;
pub use write::{
    IcebergChangelogWriter, IcebergDataFileMeta, IcebergManifestEntry, IcebergManifestFileMeta,
};
