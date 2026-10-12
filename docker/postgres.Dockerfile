# Инструменты сборки ядра PostgreSQL.
# Исходники конкретного sha монтируются снаружи. Ключ модели в образ не входит.

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bison \
        build-essential \
        ca-certificates \
        flex \
        git \
        libicu-dev \
        locales \
        perl \
        pkg-config \
        python3 \
        zlib1g-dev \
    && locale-gen en_US.UTF-8 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
