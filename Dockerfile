syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY go.mod ./
RUN go mod download 2>/dev/null || true
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/raftkv-server ./cmd/server

FROM alpine:3.20
RUN apk add --no-cache curl ca-certificates
COPY --from=builder /out/raftkv-server /usr/local/bin/raftkv-server
VOLUME ["/data"]
ENV DATA_DIR=/data
EXPOSE 7000 8000
ENTRYPOINT ["/usr/local/bin/raftkv-server"]
