# --- build stage ---
# Compile on the full Go toolchain image; this stage never ships.
FROM golang:1.26 AS build
WORKDIR /src

# Copy just the dependency manifests first so Docker can cache the
# `go mod download` layer separately from source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
# so it runs on the minimal alpine base below without needing a matching
# glibc/musl at runtime.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/tracker ./cmd/tracker

# --- run stage ---
# alpine (not distroless) deliberately: this is a learning project and
# having a shell (`docker exec -it <container> sh`) to poke around in is
# worth the extra few MB over a stripped-down image.
FROM alpine:3.20
# ca-certificates is required for the outbound HTTPS calls this service
# makes to MTA's realtime feed and static schedule endpoints.
RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=build /out/tracker ./tracker
COPY web ./web

# Matches config.go's default TRANSITPULSE_HTTP_ADDR — override at runtime
# with -e TRANSITPULSE_HTTP_ADDR=:PORT if needed.
EXPOSE 8080

ENTRYPOINT ["./tracker"]
