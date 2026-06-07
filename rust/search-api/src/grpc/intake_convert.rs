//! Conversions between intake protobuf messages and domain types.
//!
//! Validation lives here: targets are checked against the path-segment allowlist, every record id
//! must be non-empty, an upsert must carry at least one of metadata, vectors, or texts, and every
//! named vector it carries must be non-empty. Ids, metadata, and texts are transported verbatim
//! and never interpreted as expressions, so no raw SQL ever crosses the seam.

use std::collections::BTreeMap;

use crate::domain::{DatasetTarget, IntakeError, IntakeReport, Mutation, Record};
use crate::intake_pb;

/// Converts an optional proto dataset target into the validated domain target.
pub fn target_from_proto(target: Option<intake_pb::DatasetTarget>) -> Result<DatasetTarget, IntakeError> {
    let target = target.ok_or_else(|| IntakeError::invalid_argument("target is required"))?;
    let target = DatasetTarget {
        org_id: target.org_id,
        tenant_id: target.tenant_id,
        namespace: target.namespace,
    };
    target
        .validate()
        .map_err(|err| IntakeError::invalid_argument(err.message().to_string()))?;
    Ok(target)
}

/// Validates and converts one proto mutation into a domain mutation.
///
/// An upsert keeps the full record. It must carry a non-empty id and at least one of metadata,
/// vectors, or texts, and every named vector it carries must be non-empty. A delete keeps only the
/// id. An unspecified or unknown op, a missing record, or an empty id is rejected.
pub fn mutation_from_proto(mutation: intake_pb::Mutation) -> Result<Mutation, IntakeError> {
    let op = intake_pb::MutationOp::try_from(mutation.op)
        .map_err(|_| IntakeError::invalid_argument("mutation op is unknown"))?;
    let record = mutation
        .record
        .ok_or_else(|| IntakeError::invalid_argument("mutation requires a record"))?;
    validate_id(&record.id)?;
    match op {
        intake_pb::MutationOp::Upsert => {
            let vectors = vectors_from_proto(record.vectors)?;
            if record.metadata.is_empty() && vectors.is_empty() && record.texts.is_empty() {
                return Err(IntakeError::invalid_argument(
                    "upsert requires at least one of metadata, vectors, or texts",
                ));
            }
            Ok(Mutation::Upsert(Record {
                id: record.id,
                event_timestamp_ms: record.event_timestamp_ms,
                metadata: record.metadata.into_iter().collect(),
                vectors,
                texts: record.texts.into_iter().collect(),
            }))
        }
        intake_pb::MutationOp::Delete => Ok(Mutation::Delete { id: record.id }),
        intake_pb::MutationOp::Unspecified => Err(IntakeError::invalid_argument("mutation op is unspecified")),
    }
}

/// Converts the proto named-vector map into the domain map, rejecting any empty named vector.
fn vectors_from_proto(
    vectors: std::collections::HashMap<String, intake_pb::FloatVector>,
) -> Result<BTreeMap<String, Vec<f32>>, IntakeError> {
    let mut out = BTreeMap::new();
    for (name, vector) in vectors {
        if vector.values.is_empty() {
            return Err(IntakeError::invalid_argument(format!(
                "vector '{name}' must be non-empty"
            )));
        }
        out.insert(name, vector.values);
    }
    Ok(out)
}

/// The record id a proto mutation refers to, used to label a per-item rejection. Empty when the
/// mutation carries no record.
pub fn mutation_id(mutation: &intake_pb::Mutation) -> String {
    mutation
        .record
        .as_ref()
        .map(|record| record.id.clone())
        .unwrap_or_default()
}

/// Converts a domain intake report into the proto response.
pub fn report_to_proto(report: IntakeReport) -> intake_pb::MutateResponse {
    intake_pb::MutateResponse {
        accepted: report.accepted,
        rejected: report.rejected,
        errors: report
            .errors
            .into_iter()
            .map(|error| intake_pb::ItemError {
                id: error.id,
                message: error.message,
            })
            .collect(),
    }
}

/// Rejects empty record ids. Ids are otherwise opaque and transported verbatim.
fn validate_id(id: &str) -> Result<(), IntakeError> {
    if id.is_empty() {
        return Err(IntakeError::invalid_argument("record id must be non-empty"));
    }
    Ok(())
}
