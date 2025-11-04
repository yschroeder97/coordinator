fn main() {
    tonic_prost_build::configure()
        .compile_protos(
            &["proto/SingleNodeWorkerRPCService.proto"],
            &["proto"],
        )
        .unwrap_or_else(|e| {
            panic!("Failed to compile protos {:?}", e);
        });
}