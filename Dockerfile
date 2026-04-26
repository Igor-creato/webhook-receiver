FROM python:3.13-slim

WORKDIR /srv

RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.136.1 \
    uvicorn[standard]==0.46.0 \
    redis==7.4.0 \
    pymysql==1.1.2 \
    cryptography==47.0.0 \
    jinja2==3.1.6 \
    python-multipart==0.0.26 \
    httptools==0.7.1 \
    prometheus_client==0.25.0

COPY . .

ENV PYTHONPATH=/srv
ENV PYTHONUNBUFFERED=1

EXPOSE 8098 8099 9100
