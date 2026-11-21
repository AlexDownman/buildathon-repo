# Shopping Sessions: Rust pipeline.
# Build stage compiles a release binary tuned for the scorers' Intel Xeon
# (Cascade Lake or newer; see .cargo/config.toml), optimised with the
# recorded profile in pgo/. The toolchain is pinned because the profile only
# matches the compiler it was recorded with (scripts/pgo.sh re-records it).
FROM rust:1.97.0-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
COPY pgo ./pgo
ENV RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq -C profile-use=/src/pgo/merged.profdata"
RUN cargo build --release --locked && strip target/release/shopping-sessions

# Runtime stage: just the binary. The scorer runs it as uid 1000 with a
# read-only root filesystem; it only reads /data and writes /output.
FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
USER 1000:1000
CMD ["/app/shopping-sessions"]
