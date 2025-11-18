fn main() {
    tonic_build::configure()
        .compile(&["proto/SingleNodeWorkerRPCService.proto"], &["proto"])
        .unwrap();
}
