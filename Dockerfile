# beeper-intercom — a self-hosted Beeper network for your own stack:
# announcements one way, an agent the other.
#
# The bridge speaks Beeper's appservice websocket, so the container needs
# egress and nothing else: no published port, no TLS, no vhost. The HTTP API
# on 8080 is for the docker network only.
#
# Pure-Go all the way down (modernc.org/sqlite, no cgo), so the runtime image
# is a static binary plus CA certificates. There is no shell in it, which is
# why the binary knows how to probe its own health endpoint.

FROM golang:1.26-trixie AS build

WORKDIR /src

# Dependencies first: the module graph changes far less often than the code.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/beeper-intercom ./cmd/beeper-intercom

# Root, not :nonroot. The state directory is a bind mount in every real
# deployment, and a distroless image that cannot write to a freshly created
# host directory turns "docker compose up" into a chown puzzle. There is no
# shell in the image and the only network it needs is egress.
FROM gcr.io/distroless/static-debian12

COPY --from=build /out/beeper-intercom /beeper-intercom

# Config is mounted read-only; state is one SQLite file on the volume.
ENV INTERCOM_CONFIG=/config/config.yaml \
    INTERCOM_DB=/data/intercom.db

VOLUME /data
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=5 \
    CMD ["/beeper-intercom", "-health"]

ENTRYPOINT ["/beeper-intercom"]
