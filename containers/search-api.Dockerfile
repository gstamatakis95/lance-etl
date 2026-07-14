ARG BUILDER_IMAGE=rust:1.91.0-bookworm@sha256:e187887ec511b3d93e45c0231d2f0fd59f1347526c58aa86343aa83c74f3e1a9
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:7b140f374b289a7c2befc338f42ebe6441b7ea838a042bbd5acbfca6ec875818

FROM ${BUILDER_IMAGE} AS builder

ARG GIT_REVISION
WORKDIR /workspace/rust/search-api
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
EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/search-api"]
