//! Arrow-to-JSON conversion for search results.

use arrow::json::ArrayWriter;
use arrow_array::RecordBatch;
use serde_json::{Map, Value};

use crate::domain::SearchError;

/// Converts an Arrow record batch into JSON objects, one map per row.
pub fn batch_to_json_rows(batch: &RecordBatch) -> Result<Vec<Map<String, Value>>, SearchError> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }
    let mut writer = ArrayWriter::new(Vec::new());
    writer
        .write(batch)
        .map_err(|err| SearchError::internal(format!("failed to encode results as JSON: {err}")))?;
    writer
        .finish()
        .map_err(|err| SearchError::internal(format!("failed to finish JSON encoding: {err}")))?;
    let bytes = writer.into_inner();
    serde_json::from_slice::<Vec<Map<String, Value>>>(&bytes)
        .map_err(|err| SearchError::internal(format!("failed to parse encoded results: {err}")))
}
