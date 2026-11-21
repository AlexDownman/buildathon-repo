# PROBE image: same pipeline (same pinned toolchain and recorded profile as
# the real Dockerfile) plus the dataset generator and probe.sh, which times
# the pipeline and then fails the dev check on purpose so the results are
# shown. Not for a real submission.
FROM rust:1.97.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
COPY pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq -C profile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked && strip target/release/shopping-sessions
COPY gen /gen
RUN cd /gen && RUSTFLAGS="" cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
COPY --from=build /gen/target/release/generate /app/generate
COPY probe.sh /app/probe.sh
RUN chmod 755 /app/probe.sh
USER 1000:1000
CMD ["/app/probe.sh"]
