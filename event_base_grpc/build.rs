fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Emit a FileDescriptorSet alongside the generated code so the server can
    // expose gRPC Server Reflection (grpcurl / buf describe work out of the
    // box). The macro `tonic::include_file_descriptor_set!` reads this exact
    // `<name>_descriptor.bin` file from OUT_DIR.
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("event_base_descriptor.bin"))
        .compile_protos(&["protos/main.proto"], &["protos"])?;
    Ok(())
}
