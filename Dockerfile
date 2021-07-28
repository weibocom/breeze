#FROM registry.api.weibo.com/weibo_rd_common/base_image:centos7.0
#
#ENV LANG=en_US.UTF-8
#
#LABEL version="1.0" description="breeze - resource mesh"
#
#
#RUN \
#    yum install openssl -y -q && \
#    mkdir -p /data1/breeze && \
#    cd /data1/breeze && \
#    mkdir bin socks logs && \
#    cd bin
#
#COPY debug/agent/agent /data1/breeze/bin/
#
#VOLUME ["/data1/breeze/socks", "/data1/breeze/snapshot",  "/data1/breeze/logs"]
#
#WORKDIR /data1/breeze/bin/
#
#ENTRYPOINT ["/data1/breeze/bin/agent"]
#CMD ["--discovery", "vintage://static.config.api.weibo.com", "--snapshot", "/data1/breeze/snapshot", "--service-path", "/data1/breeze/socks"]


FROM registry.api.weibo.com/weibo_rd_if/breeze_builder:0.0.1 AS builder
WORKDIR /breeze
COPY . .
ADD --chown=rust:rust . ./
RUN cargo build --release

FROM registry.api.weibo.com/weibo_rd_if/breeze_base_alpine:0.0.1

RUN \
    mkdir -p /data1/breeze && \
    cd /data1/breeze && \
    mkdir bin socks logs && \
    cd bin 

COPY --from=builder /breeze/target/x86_64-unknown-linux-musl/release/agent /data1/breeze/bin/breeze

VOLUME ["/data1/breeze/socks", "/data1/breeze/snapshot",  "/data1/breeze/logs"]

WORKDIR /data1/breeze

ENTRYPOINT ["/data1/breeze/bin/breeze"]

CMD ["--discovery", "vintage://static.config.api.weibo.com", "--snapshot", "/data1/breeze/snapshot", "--service-path", "/data1/breeze/socks", "--log-dir", "/data1/breeze/logs", "--metrics-url", "logtailer.monitor.weibo.com:8333"]
