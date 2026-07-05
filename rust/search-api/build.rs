//! Compiles the single gRPC protobuf definition with `tonic-prost-build`.
//!
//! `prost-build` resolves `protoc` from `PATH` (or a `PROTOC` env override). The build host
//! provides protoc, keeping the crate free of a vendored protobuf toolchain. Both the
//! `SearchService` and the `IntakeService` live in one `lance_etl.v1` proto file.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(&["proto/lance_etl/v1/lance_etl.proto"], &["proto"])?;
    Ok(())
}
