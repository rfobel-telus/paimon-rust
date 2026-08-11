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

use std::sync::{Arc, Mutex};

use arrow::pyarrow::FromPyArrow;
use paimon::table::{Table, TableCommit, TableWrite};
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;
use uuid::Uuid;

use crate::error::to_py_err;

/// Builder for table writes and commits. Mirrors `WriteBuilder` in the core.
#[pyclass(name = "WriteBuilder", module = "pypaimon_rust.datafusion")]
pub struct PyWriteBuilder {
    table: Arc<Table>,
    commit_user: String,
    branch: Option<String>,
}

impl PyWriteBuilder {
    pub fn new(table: Arc<Table>) -> Self {
        Self {
            commit_user: Uuid::new_v4().to_string(),
            table,
            branch: None,
        }
    }
}

#[pymethods]
impl PyWriteBuilder {
    /// Override the commit user (defaults to a random UUID).
    fn with_commit_user(mut slf: PyRefMut<'_, Self>, commit_user: String) -> PyResult<PyRefMut<'_, Self>> {
        slf.commit_user = commit_user;
        Ok(slf)
    }

    /// Route snapshot commits to a named branch instead of the main table.
    ///
    /// Data files and manifests remain in the main table root. Only the
    /// snapshot landing path is redirected to
    /// `{table_root}/branch/branch-{name}/snapshot/`.
    ///
    /// The branch must already exist (create it with
    /// `table.branch_manager().create_branch(name)` first).
    fn with_branch(mut slf: PyRefMut<'_, Self>, branch_name: String) -> PyRefMut<'_, Self> {
        slf.branch = Some(branch_name);
        slf
    }

    /// Create a [`PyTableWrite`] for writing Arrow batches.
    fn new_write(&self) -> PyResult<PyTableWrite> {
        let wb = self.table.new_write_builder();
        let wb = wb
            .with_commit_user(self.commit_user.clone())
            .map_err(to_py_err)?;
        let write = wb.new_write().map_err(to_py_err)?;
        Ok(PyTableWrite {
            inner: Mutex::new(write),
        })
    }

    /// Create a [`PyTableCommit`] for committing write results.
    fn new_commit(&self) -> PyTableCommit {
        let table = (*self.table).clone();
        let commit = if let Some(ref branch) = self.branch {
            TableCommit::new_for_branch(table, self.commit_user.clone(), branch)
        } else {
            TableCommit::new(table, self.commit_user.clone())
        };
        PyTableCommit { inner: commit }
    }
}

/// Wraps [`TableWrite`] for Arrow-batch writes from Python.
#[pyclass(name = "TableWrite", module = "pypaimon_rust.datafusion")]
pub struct PyTableWrite {
    inner: Mutex<TableWrite>,
}

#[pymethods]
impl PyTableWrite {
    /// Write a PyArrow `RecordBatch` into the current epoch.
    fn write_arrow_batch(&self, py: Python<'_>, batch: &Bound<'_, PyAny>) -> PyResult<()> {
        let rb = arrow::array::RecordBatch::from_pyarrow_bound(batch)?;
        let rt = runtime();
        py.detach(|| {
            let mut write = self.inner.lock().unwrap();
            rt.block_on(write.write_arrow_batch(&rb)).map_err(to_py_err)
        })
    }

    /// Flush buffered writes and return opaque commit messages.
    ///
    /// Pass the returned [`PyCommitMessages`] to `WriteBuilder.new_commit().commit(messages)`.
    fn prepare_commit(&self, py: Python<'_>) -> PyResult<PyCommitMessages> {
        let rt = runtime();
        let messages = py.detach(|| {
            let mut write = self.inner.lock().unwrap();
            rt.block_on(write.prepare_commit()).map_err(to_py_err)
        })?;
        Ok(PyCommitMessages { inner: messages })
    }
}

/// Opaque container for [`CommitMessage`]s produced by [`PyTableWrite::prepare_commit`].
///
/// Pass directly to [`PyTableCommit::commit`] — do not inspect or modify.
#[pyclass(name = "CommitMessages", module = "pypaimon_rust.datafusion")]
pub struct PyCommitMessages {
    pub(crate) inner: Vec<paimon::table::CommitMessage>,
}

/// Wraps [`TableCommit`] for snapshot commits from Python.
#[pyclass(name = "TableCommit", module = "pypaimon_rust.datafusion")]
pub struct PyTableCommit {
    inner: TableCommit,
}

#[pymethods]
impl PyTableCommit {
    /// Commit the messages produced by [`PyTableWrite::prepare_commit`].
    fn commit(&self, py: Python<'_>, messages: &PyCommitMessages) -> PyResult<()> {
        let msgs = messages.inner.clone();
        let rt = runtime();
        py.detach(|| rt.block_on(self.inner.commit(msgs)).map_err(to_py_err))
    }
}
