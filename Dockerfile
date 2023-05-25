FROM rust:1.69-slim-bullseye AS builder

RUN mkdir /work
WORKDIR /work

ADD . /work

RUN apt -y update && apt -y install protobuf-compiler pkg-config libssl-dev

RUN cargo -vv build --release



FROM debian:bullseye-slim

RUN mkdir -p /opt/edgebit

COPY --from=builder /work/target/release/cluster-agent /opt/edgebit/

ENTRYPOINT [ "/opt/edgebit/cluster-agent" ]
