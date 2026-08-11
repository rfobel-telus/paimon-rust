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

//! CommitCallback extension point, invoked after a table commit succeeds.
//!
//! Reference: `org.apache.paimon.CommitCallback` (Java), as used by e.g.
//! `IcebergCommitCallback` / `IcebergChangelogCommitCallback` to emit
//! companion Iceberg metadata on every Paimon snapshot commit. See
//! `docs/paimon-rust-commit-callback-scoping.md` in the planning repo for
//! the ground-truth Java signature this mirrors and the design decisions
//! below.

use crate::spec::Snapshot;
use crate::Result;
use async_trait::async_trait;

/// Hook invoked once per successful logical commit — once per
/// [`TableCommit::commit`](super::TableCommit::commit) /
/// [`commit_with_identifier`](super::TableCommit::commit_with_identifier)
/// call, not once per internal retry attempt — with the snapshot that was
/// just durably committed.
///
/// Register callbacks via
/// [`TableCommit::with_commit_callbacks`](super::TableCommit::with_commit_callbacks).
///
/// A callback error fails the whole commit (propagated with `?`), rather
/// than being logged and swallowed. This is a deliberate default chosen
/// because no real callback implementation exists yet to observe actual
/// failure behavior against; it is not a confirmed match to Java's
/// `FileStoreCommitImpl` semantics. Revisit if/when that's verified.
#[async_trait]
pub trait CommitCallback: Send + Sync {
    /// Called once, after `snapshot` has been durably committed.
    async fn call(&self, snapshot: &Snapshot) -> Result<()>;

    /// Called when a commit is discovered to have already succeeded from a
    /// prior attempt, identified only by `commit_identifier`. Mirrors Java's
    /// `CommitCallback.retry(ManifestCommittable)`, used there when a
    /// two-phase-commit orchestrator (Flink's committer) re-drives a commit
    /// after a crash between the file-store commit and callback execution.
    ///
    /// Not currently invoked anywhere: `TableCommit`'s commit path is direct
    /// (batch), with no external orchestrator that re-presents an
    /// already-committed identifier. Default no-op until such a caller
    /// exists — don't build speculative call-site plumbing for it yet.
    async fn retry(&self, _commit_identifier: i64) -> Result<()> {
        Ok(())
    }
}
