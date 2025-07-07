# ── Stage 1: build ───────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o gossip-node ./gossiping_node

# ── Stage 2: runtime ─────────────────────────────────────────────────────
FROM alpine:latest

# copia il binario
COPY --from=builder /app/gossip-node /usr/local/bin/gossip-node

WORKDIR /usr/local/bin

# espone una porta di default (poi in compose passi quella giusta)
EXPOSE 9001

# qui definiamo l’eseguibile di default
ENTRYPOINT ["gossip-node"]
