# Shopping Sessions: Rust pipeline.
# Build stage compiles a release binary tuned for the scorers' Intel Xeon
# (Cascade Lake or newer, AVX-512; see .cargo/config.toml).
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked && strip target/release/shopping-sessions

# Runtime stage: just the binary. The scorer runs it as uid 1000 with a
# read-only root filesystem; it only reads /data and writes /output.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
