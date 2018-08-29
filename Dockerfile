FROM alpine
# FROM ubuntu:xenial

# RUN apt-get update && \
#     apt-get install -y libssl-dev ca-certificates --no-install-recommends && \
#     rm -rf /var/lib/apt/lists/*

RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*

COPY target/x86_64-unknown-linux-musl/debug/sqs-reader /bin/sqs-reader

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    SSL_CERT_DIR=/etc/ssl/certs

CMD ["sqs-reader"]
