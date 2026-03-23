FROM python:3.12-slim

WORKDIR /srv

RUN apt-get update && apt-get install -y --no-install-recommends wget && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    fastapi==0.115.6 \
    uvicorn[standard]==0.34.0 \
    redis==5.2.1 \
    pymysql==1.1.1 \
    cryptography==44.0.0 \
    jinja2==3.1.5 \
    python-multipart==0.0.20 \
    httptools==0.6.4

COPY . .

ENV PYTHONPATH=/srv
ENV PYTHONUNBUFFERED=1

EXPOSE 8098 8099
