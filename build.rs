const PROTOS: &[&str] = &[
    "edgebitapis/edgebit/agent/v1alpha/token_service.proto",
    "edgebitapis/edgebit/agent/v1alpha/inventory_service.proto",
];

fn build() -> Result<(), Box<dyn std::error::Error>> {
    let includes: &[&str] = &[];

    tonic_build::configure()
        .compile(PROTOS, includes)?;

    for proto in PROTOS {
        println!("cargo:rerun-if-changed={proto}");
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Err(e) = build() {
        eprintln!("{}", e.to_string());
        return Err(e);
    }
    Ok(())
}
