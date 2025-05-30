FROM rust:1.87 AS builder

RUN apt update && apt install -y protobuf-compiler libprotobuf-dev

WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY ./src ./src
COPY ./build.rs ./
COPY ./proto ./proto

RUN cargo build --release

FROM  gcr.io/distroless/cc-debian12

USER nonroot:nonroot
COPY --from=builder /usr/src/app/target/release /
CMD ["./server"]
