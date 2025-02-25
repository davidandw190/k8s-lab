FROM golang:1.20-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /image-webhook

FROM alpine:3.17

WORKDIR /
COPY --from=builder /image-webhook /image-webhook

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

EXPOSE 8080
ENTRYPOINT ["/image-webhook"]