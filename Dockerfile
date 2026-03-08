FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd/myapp

FROM alpine:latest

# Use --no-cache to keep the image small
RUN apk add --no-cache libc6-compat

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/main .
COPY --from=builder /app/internal/db/migrations ./db/migrations
COPY --from=builder /app/internal/cfg/*.yaml ./internal/cfg/

# Expose App ports
EXPOSE 8080 9090

# Run the application
CMD ["./main"]