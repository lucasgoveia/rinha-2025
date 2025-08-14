FROM rust:1.88-alpine AS builder

RUN apk add --no-cache \
        build-base \
        musl-dev \
        cmake

WORKDIR /usr/src/gateway

COPY . .
ENV RUSTFLAGS="-C target-cpu=native"
RUN cargo build --release --locked

FROM alpine:latest

ARG APP_UID=1000
ARG APP_GID=1000
RUN addgroup -g ${APP_GID} app && adduser -D -G app -u ${APP_UID} app \
 && mkdir -p /app \
 && chown -R ${APP_UID}:${APP_GID} /app /tmp

USER app
WORKDIR /app
COPY --from=builder /usr/src/gateway/target/release/gateway /app/

CMD ["./gateway"]
