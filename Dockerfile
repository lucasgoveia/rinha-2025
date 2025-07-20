FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o api ./cmd/api


FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/api .

EXPOSE 7118

CMD ["./api"]