FROM golang:1.26.1-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

COPY go.mod go.sum ./
COPY vendor/ vendor/

# No network needed — all deps are in vendor/
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -mod=vendor -v -o main ./cmd/myapp

# Final Stage
FROM alpine:latest
RUN apk add --no-cache libc6-compat
WORKDIR /app

COPY --from=builder /app/main .
COPY --from=builder /app/internal/db/migrations ./db/migrations
COPY --from=builder /app/internal/cfg/*.yaml ./internal/cfg/

EXPOSE 8080 9090

CMD ["./main"]