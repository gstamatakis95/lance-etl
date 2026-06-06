//! Compiles the gRPC protobuf definitions with `tonic-prost-build`.
//!
//! `prost-build` resolves `protoc` from `PATH` (or a `PROTOC` env override); the build host
//! provides protoc, keeping the crate free of a vendored protobuf toolchain.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(&["proto/lance_etl/search/v1/search.proto"], &["proto"])?;
    Ok(())
}
