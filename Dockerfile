# syntax=docker/dockerfile:1

# ---- build stage: compile static binaries ----
FROM golang:1.26 AS build
WORKDIR /src

# Cache module downloads separately from the source for faster rebuilds.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO off + static linking so the binaries run on distroless/static.
ENV CGO_ENABLED=0
RUN go build -ldflags="-s -w" -o /out/worker    ./cmd/worker  && \
    go build -ldflags="-s -w" -o /out/dashboard ./cmd/dashboard && \
    go build -ldflags="-s -w" -o /out/enqueue   ./cmd/enqueue

# ---- run stage: tiny, non-root, no shell ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
# Default to the dashboard; override `command:` (compose) / `args` (k8s) for the worker.
ENTRYPOINT ["/usr/local/bin/dashboard"]
