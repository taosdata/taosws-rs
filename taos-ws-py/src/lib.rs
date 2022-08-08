use pyo3::types::{PyTuple, PyDict};
use pyo3::PyIterProtocol;
use pyo3::{create_exception, exceptions::PyException};
use pyo3::{prelude::*, PyObjectProtocol};
use taos_query::prelude::sync::*;
use taos_query::{
    common::RawBlock as Block,
    prelude::BorrowedValue,
    Fetchable,
};
use taos_ws::{Taos, TaosBuilder, ResultSet};

create_exception!(taosws, ConnectionError, PyException);
create_exception!(taosws, QueryError, PyException);
create_exception!(taosws, FetchError, PyException);

#[pyclass]
struct TaosConnection {
    builder: TaosBuilder,
    cursor: Option<TaosCursor>,
}

#[pyclass]
struct TaosCursor {
    _description: Option<String>,
    _inner: Taos,
    _rowcount: i32,
    _close: bool,
    #[pyo3(get, set)]
    _arraysize: i32,
    _result: Option<ResultSet>,
}

#[pyclass]
struct TaosField {
    _inner: Field,
}

impl TaosField {
    fn new(inner: &Field) -> Self {
        Self {
            _inner: inner.clone(),
        }
    }
}

#[pymethods]
impl TaosField {
    fn name(&self) -> &str {
        self._inner.name()
    }

    fn r#type(&self) -> &str {
        self._inner.ty().name()
    }

    fn bytes(&self) -> u32 {
        self._inner.bytes()
    }
}

#[pyproto]
impl PyObjectProtocol for TaosField {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }

    fn __str__(&self) -> PyResult<String> {
        Ok(format!(
            "{{name: {}, type: {}, bytes: {}}}",
            self.name(),
            self.r#type(),
            self.bytes()
        ))
    }
}

#[pyclass]
struct TaosResult {
    _inner: ResultSet,
    _block: Option<Block>,
    _current: usize,
    _num_of_fields: i32,
}

#[pymethods]
impl TaosConnection {
    fn query(&mut self, sql: &str) -> PyResult<TaosResult> {
        match self.cursor {
            Some(_) => {},
            None => self.cursor = Some(self.cursor().unwrap()),
        };
        if let Some(cursor) = self.cursor.as_ref() {
            match cursor._inner.query(sql) {
                Ok(rs) => {
                    let cols = rs.num_of_fields();
                    Ok(TaosResult {
                        _inner: rs,
                        _block: None,
                        _current: 0,
                        _num_of_fields: cols as _,
                    })
                }
                Err(err) => Err(QueryError::new_err(err.errstr())),
            }
        } else {
            Err(QueryError::new_err("cannot create cursor"))
        }
    }

    fn execute(&mut self, sql: &str) -> PyResult<i32> {
        match self.cursor {
            Some(_) => {},
            None => self.cursor = Some(self.cursor().unwrap()),
        };
        if let Some(cursor) = self.cursor.as_ref() {
            match cursor._inner.query(sql) {
                Ok(rs) => Ok(rs.affected_rows()),
                Err(err) => Err(QueryError::new_err(err.errstr())),
            }
        } else {
            Err(QueryError::new_err("cannot create cursor"))
        }
    }

    fn close(&self) {
        // self.cursor._inner.close();
    }

    fn commit(&self) {}

    fn rollback(&self) {}

    fn cursor(&self) -> PyResult<TaosCursor> {
        Ok(TaosCursor { 
            _inner: self.builder.build().map_err(|err| ConnectionError::new_err(err.to_string()))?,
            _description: None,
            _rowcount: 0,
            _close: false,
            _arraysize: 1,
            _result: None,
        })
    }
}

#[pymethods]
impl TaosCursor {
    #[getter]
    fn description(&self) -> PyResult<Vec<(String, u8)>> {
        if self._close {
            Err(ConnectionError::new_err("cursor already closed"))
        } else {
            if let Some(rs) = self._result.as_ref() {
                Ok(rs.fields().into_iter().map(|f| (f.name().to_string(),f.ty() as u8)).collect::<Vec<_>>())
            } else {
                Ok(vec![])
            }
        }
    }

    #[getter]
    fn rowcount(&self) -> PyResult<i32> {
        if self._close {
            Err(ConnectionError::new_err("cursor already closed"))
        } else {
            Ok(self._rowcount)
        }
    }
    #[args(
        py_kwargs = "**"
    )]
    fn execute(&mut self, operation: &str, _py_kwargs: Option<&PyDict>) -> PyResult<()>{
        self._result = Some(self._inner.query(operation).map_err(|err| QueryError::new_err(err.errstr()))?);
        Ok(())
    }

    #[args(
        py_kwargs = "**"
    )]
    fn executemany(&mut self, operation: &str, py_kwargs: Option<&PyDict>) -> PyResult<()> {
        self.execute(operation, py_kwargs)
    }

    fn close(&mut self) {
        self._close = true;
    }

    fn fetch_one(&self) {}

    fn fetch_many(&self) {}

    fn fetchall (&mut self) -> PyResult<Vec<PyObject>> {
        let mut ret = Vec::<PyObject>::new();
        if let Some(res) = self._result.as_mut() {
            if let Some(block) = res.fetch_raw_block().unwrap_or_default() {
                convert_raw_block_to_python_tuple(&mut ret, &Some(block));
                loop {
                    if let Some(block) = res.fetch_raw_block().unwrap_or_default() {
                        convert_raw_block_to_python_tuple(&mut ret, &Some(block));
                    } else {
                        break;
                    }
                }
                Ok(ret)
            } else {
                Err(FetchError::new_err("find no result in result set."))
            }
        } else {
            Err(FetchError::new_err("not generate result set before fetch."))
        }
    }

    fn next_set(&self) {}

}

fn convert_raw_block_to_python_tuple(ret: &mut Vec<PyObject>, block: &Option<Block>) {
    Python::with_gil(move |py| {
        if let Some(block) = block.as_ref() {
            for row in 0..block.nrows() {
                let mut vec = Vec::new();
                for col in 0..block.ncols() {
                    let value = block.get_ref(row, col).unwrap();
                    let value = match value {
                        BorrowedValue::Null => Option::<()>::None.into_py(py),
                        BorrowedValue::Bool(v) => v.into_py(py),
                        BorrowedValue::TinyInt(v) => v.into_py(py),
                        BorrowedValue::SmallInt(v) => v.into_py(py),
                        BorrowedValue::Int(v) => v.into_py(py),
                        BorrowedValue::BigInt(v) => v.into_py(py),
                        BorrowedValue::UTinyInt(v) => v.into_py(py),
                        BorrowedValue::USmallInt(v) => v.into_py(py),
                        BorrowedValue::UInt(v) => v.into_py(py),
                        BorrowedValue::UBigInt(v) => v.into_py(py),
                        BorrowedValue::Float(v) => v.into_py(py),
                        BorrowedValue::Double(v) => v.into_py(py),
                        BorrowedValue::Timestamp(ts) => {
                            ts.to_datetime_with_tz().to_string().into_py(py)
                        }
                        BorrowedValue::VarChar(s) => s.into_py(py),
                        BorrowedValue::NChar(v) => v.as_ref().into_py(py),
                        BorrowedValue::Json(j) => std::str::from_utf8(&j).unwrap().into_py(py),
                        _ => Option::<()>::None.into_py(py),
                    };
                    vec.push(value);
                }
                ret.push(PyTuple::new(py, vec).to_object(py));
            }
        }
    })
}

#[pyproto]
impl PyIterProtocol for TaosResult {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<Self>) -> Option<PyObject> {
        if let Some(block) = slf._block.as_ref() {
            if slf._current >= block.nrows() {
                slf._block = slf._inner.fetch_raw_block().unwrap_or_default();
            }
        } else {
            slf._block = slf._inner.fetch_raw_block().unwrap_or_default();
        }
        Python::with_gil(|py| -> Option<PyObject> {
            if let Some(block) = slf._block.as_ref() {
                let mut vec = Vec::new();
                for col in 0..block.ncols() {
                    let value = block.get_ref(slf._current, col).unwrap();
                    let value = match value {
                        BorrowedValue::Null => Option::<()>::None.into_py(py),
                        BorrowedValue::Bool(v) => v.into_py(py),
                        BorrowedValue::TinyInt(v) => v.into_py(py),
                        BorrowedValue::SmallInt(v) => v.into_py(py),
                        BorrowedValue::Int(v) => v.into_py(py),
                        BorrowedValue::BigInt(v) => v.into_py(py),
                        BorrowedValue::UTinyInt(v) => v.into_py(py),
                        BorrowedValue::USmallInt(v) => v.into_py(py),
                        BorrowedValue::UInt(v) => v.into_py(py),
                        BorrowedValue::UBigInt(v) => v.into_py(py),
                        BorrowedValue::Float(v) => v.into_py(py),
                        BorrowedValue::Double(v) => v.into_py(py),
                        BorrowedValue::Timestamp(ts) => {
                            ts.to_datetime_with_tz().to_string().into_py(py)
                        }
                        BorrowedValue::VarChar(s) => s.into_py(py),
                        BorrowedValue::NChar(v) => v.as_ref().into_py(py),
                        BorrowedValue::Json(j) => std::str::from_utf8(&j).unwrap().into_py(py),
                        _ => Option::<()>::None.into_py(py),
                    };
                    vec.push(value);
                }
                slf._current += 1;
                return Some(PyTuple::new(py, vec).to_object(py));
            }
            None
        })
    }
}

#[pymethods]
impl TaosResult {
    #[getter]
    fn fields(&self) -> PyResult<Vec<TaosField>> {
        Ok(self
            ._inner
            .fields()
            .into_iter()
            .map(TaosField::new)
            .collect())
    }

    #[getter]
    fn field_count(&self) -> PyResult<i32> {
        Ok(self._num_of_fields)
    }
}

#[pyfunction]
fn connect(dsn: &str) -> PyResult<TaosConnection> {
    let builder = TaosBuilder::from_dsn(dsn).map_err(|err| ConnectionError::new_err(err.to_string()))?;
    Ok(TaosConnection {
        builder,
        cursor: None,
    })
}

#[pymodule]
fn taosws(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<TaosConnection>()?;
    m.add_class::<TaosCursor>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add("ConnectionError", py.get_type::<ConnectionError>())?;
    m.add("QueryError", py.get_type::<QueryError>())?;
    m.add("FetchError", py.get_type::<FetchError>())?;
    Ok(())
}
