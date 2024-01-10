FROM rust:1.70-slim-bookworm AS builder

RUN apt -y update && apt -y install protobuf-compiler pkg-config libssl-dev

RUN rustup component add rustfmt

ADD . /work

WORKDIR /work

RUN cargo fmt --check && cargo build --release

# ---------------------------

FROM cgr.dev/chainguard/glibc-dynamic:latest

COPY --from=builder /work/target/release/cluster-agent /app/cluster-agent

# Copy the lock file for SBOM to include Rust packages
COPY --from=builder /work/Cargo.lock /app/

ENTRYPOINT [ "/app/cluster-agent" ]
