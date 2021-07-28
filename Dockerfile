FROM registry.api.weibo.com/weibo_rd_common/base_image:centos7.0
  
ENV LANG=en_US.UTF-8

LABEL version="1.0" description="breeze - resource mesh"


RUN \
    yum install openssl -y -q && \
    mkdir -p /data1/breeze && \
    cd /data1/breeze && \
    mkdir bin socks logs && \
    cd bin 

COPY debug/agent/agent /data1/breeze/bin/

VOLUME ["/data1/breeze/socks", "/data1/breeze/snapshot",  "/data1/breeze/logs"]

WORKDIR /data1/breeze/bin/

ENTRYPOINT ["/data1/breeze/bin/agent"]
CMD ["--discovery", "vintage://static.config.api.weibo.com", "--snapshot", "/data1/breeze/snapshot", "--service-path", "/data1/breeze/socks"]
