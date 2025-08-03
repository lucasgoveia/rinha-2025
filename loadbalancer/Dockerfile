FROM rust:1.88-alpine AS builder

RUN apk add --no-cache \
        build-base \
        musl-dev \
        cmake

WORKDIR /usr/src/loadbalancer
COPY . .
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --locked

FROM alpine:latest

ARG APP_UID=1000
RUN addgroup -g ${APP_UID} appgroup && \
    adduser -u ${APP_UID} -G appgroup -D appuser

WORKDIR /app
COPY --from=builder /usr/src/loadbalancer/target/release/loadbalancer /app/

RUN chown -R appuser:appgroup /tmp

USER appuser
CMD ["./loadbalancer"]
