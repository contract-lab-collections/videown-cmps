FROM golang:1.19.2-alpine3.16 AS builder

ARG go_proxy
ENV GOPROXY ${go_proxy}
ENV https_proxy ${https_proxy}
ENV http_proxy ${http_proxy}

RUN set -eux; \
  apk add make git
  #apk add build-base make git

# Download packages first so they can be cached.
COPY go.mod go.sum /opt/target/
RUN cd /opt/target/ && go mod download

COPY . /opt/target/

# Build the thing.
RUN cd /opt/target/ \
  && make

FROM golang:1.19.2-alpine3.16
WORKDIR /opt/cmps
COPY --from=builder /opt/target/cmps ./
COPY --from=builder /opt/target/static ./static