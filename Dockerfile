# syntax=docker/dockerfile:1.6
#
# Multi-stage build for parser-worker. Final image is distroless/static so it
# runs as a non-root user, has no shell, and stays under ~25 MB.
#
# Build:
#   docker build -t parser-worker .
# Run:
#   docker run --rm --env-file .env parser-worker
# Or with daemon mode:
#   docker run -d --restart=unless-stopped --env-file .env --name parser-worker parser-worker

# ---- builder ---------------------------------------------------------------

FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache deps separately from sources so a code-only change doesn't re-download
# the Go module graph.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# CGO_ENABLED=0 + -trimpath gives us a fully static binary safe for distroless.
# -ldflags='-s -w' strips debug info to keep the image small.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags='-s -w' \
      -o /out/parser-worker ./cmd/worker

# ---- runtime ---------------------------------------------------------------

FROM gcr.io/distroless/static-debian12:nonroot

# /tmp is required for archive downloads. Distroless ships with it but make
# the expectation explicit.
WORKDIR /app

COPY --from=builder /out/parser-worker /app/parser-worker

USER nonroot:nonroot

ENTRYPOINT ["/app/parser-worker"]
