# syntax=docker/dockerfile:1

# ---- Build stage ----
# go.mod requires go 1.27.3; keep this in sync with go.mod.
FROM golang:1.27-alpine AS builder

# gcc/musl-dev are required: gofluxer -> mattn/go-sqlite3 uses CGO.
RUN apk add --no-cache gcc musl-dev

WORKDIR /src

# Cache module downloads separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=1 is required for mattn/go-sqlite3.
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o /out/franz-hermann .

# ---- Runtime stage ----
FROM alpine:3.22

# ca-certificates for https://api.openf1.org, tzdata for time.Local.
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 app \
    && mkdir -p /app/data \
    && chown app:app /app/data

WORKDIR /app

COPY --from=builder /out/franz-hermann ./franz-hermann

# data/ is a writable cache dir (race.json, standings.json, ...).
# In compose it is persisted via a volume, so chown survives.
USER app

# No ports exposed: this is a Discord bot (outbound websocket only).
# BOT_TOKEN is injected at runtime via env_file (.env), never baked in.
CMD ["./franz-hermann"]
