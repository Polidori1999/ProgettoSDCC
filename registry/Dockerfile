# registry/Dockerfile

FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod tidy
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o registry registry.go

FROM alpine:latest
COPY --from=builder /app/registry /usr/local/bin/registry
EXPOSE 9000
ENTRYPOINT ["registry"]
