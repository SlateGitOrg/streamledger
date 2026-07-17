# syntax=docker/dockerfile:1

FROM golang:1.25-bookworm AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/streamledger ./cmd/streamledger

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/streamledger /usr/local/bin/streamledger

# gRPC command/query API and Prometheus /metrics respectively; see
# cmd/streamledger's -addr/-metrics-addr flags to change them.
EXPOSE 9090 9091

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/streamledger"]
