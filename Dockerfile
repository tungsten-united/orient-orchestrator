# One image, two binaries: the orchestrator (default command) and the fakes (staging only, `--command fakes`).
FROM rust:1.99-slim-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked --bin orient-orchestrator --example fakes

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/orient-orchestrator /src/target/release/examples/fakes /usr/local/bin/
# Cloud Run sends traffic to 8080. Both binaries listen on BIND.
ENV BIND=0.0.0.0:8080
EXPOSE 8080
USER nobody
CMD ["orient-orchestrator"]
