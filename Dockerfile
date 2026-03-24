# Dockerfile
# Stage 1: Build environment
FROM debian:bookworm-slim AS builder

RUN apt-get update && apt-get install -y \
    build-essential \
    libcurl4-openssl-dev \
    libjansson-dev \
    libmicrohttpd-dev \
    libzmq3-dev \
    libsodium-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Compile the gateway
RUN make clean && make

# Stage 2: Runtime environment
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libcurl4 \
    libjansson4 \
    libmicrohttpd12 \
    libzmq5 \
    libsodium23 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled executable from the builder stage
COPY --from=builder /app/solo_gateway /app/solo_gateway

# Expose Stratum port and Web API port
EXPOSE 3333 7152

# Run the executable, defaulting to the config file mounted in /app
ENTRYPOINT ["/app/solo_gateway", "-c", "/app/datum_gateway_config.json"]
