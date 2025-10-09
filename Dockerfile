# Dockerfile (root)

FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o gossip-node ./gossiping_node



FROM alpine:latest
COPY --from=builder /app/gossip-node /usr/local/bin/gossip-node
WORKDIR /usr/local/bin
EXPOSE 9001/udp 9002/udp 9003/udp 9004/udp 9005/udp 9006/udp
ENTRYPOINT ["gossip-node"]
