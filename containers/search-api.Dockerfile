ARG BUILDER_IMAGE=rust:1.91.0-bookworm@sha256:e187887ec511b3d93e45c0231d2f0fd59f1347526c58aa86343aa83c74f3e1a9
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

FROM ${BUILDER_IMAGE} AS builder

ARG GIT_REVISION
WORKDIR /workspace/rust/search-api
ADD --checksum=sha256:b24b53f87c151bfd48b112fe4c3a6e6574e5198874f38036aff41df3456b8caf \
    https://github.com/protocolbuffers/protobuf/releases/download/v33.2/protoc-33.2-linux-x86_64.zip \
    /tmp/protoc-amd64.zip
ADD --checksum=sha256:706662a332683aa2fffe1c4ea61588279d31679cd42d91c7d60a69651768edb8 \
    https://github.com/protocolbuffers/protobuf/releases/download/v33.2/protoc-33.2-linux-aarch_64.zip \
    /tmp/protoc-arm64.zip
RUN architecture="$(dpkg --print-architecture)" \
    && test "${architecture}" = amd64 -o "${architecture}" = arm64 \
    && unzip -q "/tmp/protoc-${architecture}.zip" -d /usr/local \
    && protoc --version | grep -Fx 'libprotoc 33.2'
COPY rust/search-api/Cargo.toml rust/search-api/Cargo.lock rust/search-api/build.rs ./
COPY rust/search-api/proto ./proto
COPY rust/search-api/src ./src
RUN test -n "${GIT_REVISION}" && cargo build --release --locked

FROM ${RUNTIME_IMAGE} AS runtime

ARG GIT_REVISION
LABEL org.opencontainers.image.source="https://github.com/gstamatakis95/lance-etl" \
      org.opencontainers.image.revision="${GIT_REVISION}" \
      org.opencontainers.image.version="${GIT_REVISION}" \
      io.lance-etl.lance.version="8.0.0"
COPY --from=builder /workspace/rust/search-api/target/release/search-api /usr/local/bin/search-api
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN mkdir -p /var/cache/search-api && chown 65532:65532 /var/cache/search-api
ENV SEARCH_API_CACHE_DIR=/var/cache/search-api
EXPOSE 8080 8081
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/search-api"]
