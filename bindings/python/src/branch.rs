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

use paimon::table::BranchManager;
use paimon_datafusion::runtime::runtime;
use pyo3::prelude::*;

use crate::error::to_py_err;

#[pyclass(name = "BranchManager", module = "pypaimon_rust.datafusion")]
pub struct PyBranchManager {
    inner: BranchManager,
}

impl PyBranchManager {
    pub fn new(inner: BranchManager) -> Self {
        Self { inner }
    }
}

#[pymethods]
impl PyBranchManager {
    /// Create a new empty branch (copies latest schema, no snapshot).
    fn create_branch(&self, py: Python<'_>, branch_name: String) -> PyResult<()> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.create_branch(&branch_name))
                .map_err(to_py_err)
        })
    }

    /// Create a branch rooted at an existing tag (copies snapshot + schema).
    fn create_branch_from_tag(
        &self,
        py: Python<'_>,
        branch_name: String,
        tag_name: String,
    ) -> PyResult<()> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.create_branch_from_tag(&branch_name, &tag_name))
                .map_err(to_py_err)
        })
    }

    /// Return True if the branch exists.
    fn branch_exists(&self, py: Python<'_>, branch_name: String) -> PyResult<bool> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.branch_exists(&branch_name))
                .map_err(to_py_err)
        })
    }

    /// List all branch names in sorted order.
    fn list_all(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.list_all()).map_err(to_py_err)
        })
    }

    /// Drop an existing branch and all its files.
    fn drop_branch(&self, py: Python<'_>, branch_name: String) -> PyResult<()> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.drop_branch(&branch_name))
                .map_err(to_py_err)
        })
    }

    /// Rename a branch.
    fn rename_branch(
        &self,
        py: Python<'_>,
        from_name: String,
        to_name: String,
    ) -> PyResult<()> {
        let rt = runtime();
        py.detach(|| {
            rt.block_on(self.inner.rename_branch(&from_name, &to_name))
                .map_err(to_py_err)
        })
    }
}
