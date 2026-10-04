use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use yt_engine::{EngineJsonApi, WriteCoordinator};

fn py_runtime_err(message: impl Into<String>) -> PyErr {
    PyRuntimeError::new_err(message.into())
}

fn py_value_err(message: impl Into<String>) -> PyErr {
    PyValueError::new_err(message.into())
}

fn parse_tenant_id(tenant_id: Option<String>) -> PyResult<Option<u64>> {
    match tenant_id {
        None => Ok(None),
        Some(s) if s.trim().is_empty() => Ok(None),
        Some(s) => s
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| py_value_err(format!("bad tenant_id: {s}"))),
    }
}

#[pyclass(name = "NativeYiTraceDB")]
pub struct NativeYiTraceDb {
    coord: Arc<WriteCoordinator>,
    api: EngineJsonApi,
    closed: bool,
}

#[pymethods]
impl NativeYiTraceDb {
    #[new]
    pub fn new(py: Python<'_>, data_dir: String) -> PyResult<Self> {
        let dir = PathBuf::from(data_dir);
        let coord = py.detach(move || {
            std::fs::create_dir_all(&dir)
                .map_err(|e| py_runtime_err(format!("create data dir failed: {e}")))?;
            let coord = WriteCoordinator::open_durable(&dir)
                .map_err(|e| {
                    py_runtime_err(format!(
                        "open yiTrace data dir failed for data_dir={}: {e}. If startup or writes are slow, check db.lock_metrics() or YiTraceRuntime.health()['lock']; lock timeouts include the owner process.",
                        dir.display()
                    ))
                })?;
            coord.try_recover().map_err(|e| py_runtime_err(format!("recover yiTrace failed: {e}")))?;
            Ok::<_, PyErr>(coord)
        })?;
        let api = EngineJsonApi::new(Arc::clone(&coord));
        Ok(Self {
            coord,
            api,
            closed: false,
        })
    }

    #[pyo3(signature = (method, path, body = "", tenant_id = None))]
    pub fn route_json(
        &self,
        py: Python<'_>,
        method: &str,
        path: &str,
        body: &str,
        tenant_id: Option<String>,
    ) -> PyResult<String> {
        self.ensure_open()?;
        let tenant = parse_tenant_id(tenant_id)?;
        let api = self.api.clone();
        let method = method.to_string();
        let path = path.to_string();
        let body = body.to_string();
        let (status, response) =
            py.detach(move || api.route_with_tenant(&method, &path, &body, tenant));
        if (200..300).contains(&status) {
            Ok(response)
        } else {
            Err(py_runtime_err(format!(
                "yiTrace request failed: status={status} body={response}"
            )))
        }
    }

    #[pyo3(signature = (trace_id, span_id, embedding, tenant_id = None))]
    pub fn index_embedding(
        &self,
        py: Python<'_>,
        trace_id: String,
        span_id: String,
        embedding: Vec<f32>,
        tenant_id: Option<String>,
    ) -> PyResult<()> {
        self.ensure_open()?;
        let tenant = parse_tenant_id(tenant_id)?;
        fn parse_id(value: &str) -> PyResult<u64> {
            if value.trim().is_empty() {
                return Err(py_value_err("trace/span id must not be empty"));
            }
            Ok(value.trim().parse().unwrap_or_else(|_| {
                value
                    .trim()
                    .as_bytes()
                    .iter()
                    .fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
                        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
                    })
            }))
        }
        let trace = parse_id(&trace_id)?;
        let span = parse_id(&span_id)?;
        let coord = Arc::clone(&self.coord);
        py.detach(move || coord.index_embedding_for_tenant(tenant, trace, span, embedding))
            .map_err(|e| py_runtime_err(e.to_string()))
    }

    pub fn flush(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_open()?;
        let coord = Arc::clone(&self.coord);
        py.detach(move || coord.try_flush_memtable())
            .map_err(|e| py_runtime_err(format!("flush yiTrace failed: {e}")))?;
        Ok(())
    }

    pub fn lock_metrics_json(&self) -> PyResult<String> {
        self.ensure_open()?;
        Ok(self.coord.process_lock_metrics_json())
    }

    pub fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        let coord = Arc::clone(&self.coord);
        py.detach(move || coord.try_flush_memtable())
            .map_err(|e| py_runtime_err(format!("flush yiTrace failed: {e}")))?;
        self.closed = true;
        Ok(())
    }

    fn ensure_open(&self) -> PyResult<()> {
        if self.closed {
            Err(py_runtime_err("YiTraceDB is closed"))
        } else {
            Ok(())
        }
    }
}

impl Drop for NativeYiTraceDb {
    fn drop(&mut self) {
        if !self.closed {
            if let Err(err) = self.coord.try_flush_memtable() {
                eprintln!("yiTrace close flush failed: {err}");
            }
            self.closed = true;
        }
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<NativeYiTraceDb>()?;
    Ok(())
}
