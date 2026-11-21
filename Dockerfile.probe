# PROBE image: same pipeline plus the dataset generator and probe.sh, which
# times the pipeline on a generated 20M-event set and then fails the dev
# check on purpose so the logs are shown. Not for a real submission.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY src ./src
RUN cargo build --release --locked && strip target/release/shopping-sessions
COPY gen /gen
RUN cd /gen && cargo build --release && strip target/release/generate

FROM debian:bookworm-slim
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
COPY --from=build /gen/target/release/generate /app/generate
COPY probe.sh /app/probe.sh
RUN chmod 755 /app/probe.sh
USER 1000:1000
CMD ["/app/probe.sh"]
