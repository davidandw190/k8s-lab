# Build stage
FROM golang:1.21-alpine AS builder
RUN apk --no-cache add ca-certificates git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o /go/bin/webhook

# Runtime stage
FROM alpine:3.19
RUN apk --no-cache add ca-certificates tzdata
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY --from=builder /go/bin/webhook /app/webhook
RUN chown -R appuser:appgroup /app
USER appuser
EXPOSE 8080
ENTRYPOINT ["/app/webhook"]