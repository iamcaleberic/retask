fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/recording_storage_service.proto")?;
    Ok(())
}
