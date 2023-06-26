FROM rust:1.70-slim-bookworm AS builder

RUN apt -y update && apt -y install protobuf-compiler pkg-config libssl-dev

RUN rustup component add rustfmt

RUN mkdir /work
WORKDIR /work

ADD . /work

RUN cargo fmt --check && cargo build --release

# ---------------------------

FROM debian:bookworm-slim

RUN apt -y update && apt -y install openssl

RUN mkdir -p /opt/edgebit

COPY --from=builder /work/target/release/cluster-agent /opt/edgebit/

# Copy the lock file for SBOM to include Rust packages
COPY --from=builder /work/Cargo.lock /opt/edgebit

ENTRYPOINT [ "/opt/edgebit/cluster-agent" ]
