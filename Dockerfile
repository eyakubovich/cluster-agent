FROM rust:1.69-slim-bullseye AS builder

RUN apt -y update && apt -y install protobuf-compiler pkg-config libssl-dev

RUN rustup component add rustfmt

RUN mkdir /work
WORKDIR /work

ADD . /work

RUN cargo fmt --check && cargo build --release

# ---------------------------

FROM debian:bullseye-slim

RUN mkdir -p /opt/edgebit

COPY --from=builder /work/target/release/cluster-agent /opt/edgebit/

ENTRYPOINT [ "/opt/edgebit/cluster-agent" ]
