#FROM harbor.magiclab.top/magiclab-base/python:3.12-slim

# 更新源、安装软件、清除缓存一把梭
#RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
#    apt-get update && apt-get install -y --no-install-recommends \
#    skopeo \
#    ca-certificates \
#    && rm -rf /var/lib/apt/lists/*


FROM harbor.magiclab.top/magiclab-base/python:3.12-slim-skopeo
WORKDIR /app
COPY app.py /app/app.py

ENV DOCKERSYNC_HOST=0.0.0.0
ENV DOCKERSYNC_PORT=8080
ENV DOCKERSYNC_DATA_DIR=/app/data
ENV PYTHONUNBUFFERED=1
VOLUME ["/app/data"]
EXPOSE 8080

CMD ["python", "-u", "/app/app.py"]