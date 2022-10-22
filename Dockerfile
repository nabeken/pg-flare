# syntax=docker/dockerfile:1.3
FROM golang:1.19 as build

ENV GOMODCACHE /root/.cache/gomod

WORKDIR /go/src
COPY . ./

RUN --mount=type=cache,target=/root/.cache \
  go build -v -o /go/bin/flare ./cmd/flare

FROM gcr.io/distroless/base-debian11:latest
COPY --from=build /go/bin/flare /
CMD ["/flare"]
