FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY .. .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o server ./cmd/server


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/server .

EXPOSE 7118

CMD ["./server"]