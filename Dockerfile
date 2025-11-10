FROM docker:27-dind

ENV GO_VERSION=1.23.1
ENV PATH="/usr/local/go/bin:${PATH}"
ENV GOPATH="/go"
ENV PATH="${GOPATH}/bin:${PATH}"

RUN apk update && apk add --no-cache \
    bash \
    make \
    git \
    sudo \
    wget \
    tar \
    gcc \
    musl-dev \
    linux-headers \
    perl \
    python3 \
    util-linux \
    kmod \
    procps \
    coreutils

RUN wget https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go${GO_VERSION}.linux-amd64.tar.gz && \
    rm go${GO_VERSION}.linux-amd64.tar.gz

# RDT support commented out - requires specific Intel hardware and kernel support
# RUN apk add --no-cache --repository=http://dl-cdn.alpinelinux.org/alpine/edge/testing \
#     intel-cmt-cat
# 
# RUN mkdir -p /sys/fs/resctrl && \
#     mkdir -p /var/lock /run/lock

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN make build
RUN chmod +x container-bench
RUN mkdir -p /app/benchmarks/examples
COPY setup/tools/post-setup.sh /tmp/post-setup.sh
RUN chmod +x /tmp/post-setup.sh
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bash"]