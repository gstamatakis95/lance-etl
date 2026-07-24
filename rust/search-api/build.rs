//! Compiles the public and internal gRPC protobuf definitions with `tonic-prost-build`.
//!
//! `prost-build` resolves `protoc` from `PATH` (or a `PROTOC` env override). The build host
//! provides protoc, keeping the crate free of a vendored protobuf toolchain. The `SearchService`
//! lives in `lance_etl.v1`, while replica-local administration lives in
//! `lance_etl.internal.v1`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let internal_output = std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("internal");
    std::fs::create_dir_all(&internal_output)?;
    tonic_prost_build::configure().compile_protos(&["proto/lance_etl/v1/lance_etl.proto"], &["proto"])?;
    tonic_prost_build::configure()
        .extern_path(".lance_etl.v1", "crate::pb")
        .out_dir(internal_output)
        .compile_protos(&["proto/lance_etl/internal/v1/admin.proto"], &["proto"])?;
    Ok(())
}
