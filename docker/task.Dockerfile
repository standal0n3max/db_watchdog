# Образ задачи: только toolchain PostgreSQL, harness и OpenCode.
# Исходники head_sha монтируются через stdin/архив, эталон в образ не входит.
# Версия OpenCode фиксируется аргументом и записывается в manifest запуска.

FROM debian:bookworm-slim

ARG TARGETARCH
ARG OPENCODE_VERSION=v1.18.20

ENV DEBIAN_FRONTEND=noninteractive \
    LANG=C.UTF-8 \
    LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends \
        bison build-essential ca-certificates curl flex git gosu libicu-dev \
        libssl3 locales openssl perl pkg-config python3 python3-venv ripgrep tzdata zlib1g-dev \
    && localedef -i de_DE -f UTF-8 de_DE.UTF-8 \
    && localedef -i en_US -f UTF-8 en_US.UTF-8 \
    && localedef -i sv_SE -f UTF-8 sv_SE.UTF-8 \
    && localedef -i tr_TR -f UTF-8 tr_TR.UTF-8 \
    && localedef -i de_DE -f UTF-8 de_DE \
    && localedef -i en_US -f UTF-8 en_US \
    && localedef -i sv_SE -f UTF-8 sv_SE \
    && localedef -i tr_TR -f UTF-8 tr_TR \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 1000 bench

ARG OPENCODE_VERSION=v1.18.20
RUN set -eu; \
    case "$TARGETARCH" in \
      arm64) target=linux-arm64 ;; \
      amd64) target=linux-x64 ;; \
      *) echo "unsupported TARGETARCH=$TARGETARCH" >&2; exit 2 ;; \
    esac; \
    curl -fsSL -o /tmp/opencode.tar.gz \
      "https://github.com/anomalyco/opencode/releases/download/${OPENCODE_VERSION}/opencode-${target}.tar.gz"; \
    mkdir -p /tmp/opencode; \
    tar -xzf /tmp/opencode.tar.gz -C /tmp/opencode; \
    find /tmp/opencode -type f -perm -u+x -exec install -m 0755 {} /usr/local/bin/ \; ; \
    rm -rf /tmp/opencode /tmp/opencode.tar.gz; \
    opencode --version

# Эффективный конфиг OpenCode генерируется на каждый run и передаётся
# через docker exec/write, а не берётся из образа. Каталог конфига готовим.
COPY docker/task-entrypoint.sh /usr/local/bin/bench-entry
RUN chmod +x /usr/local/bin/bench-entry \
    && mkdir -p /workspace/postgres /work /home/bench/.config/opencode \
    && chown -R bench:bench /workspace /work /home/bench

ENV OPENCODE_CONFIG=/home/bench/.config/opencode/opencode.jsonc \
    OPENCODE_DISABLE_MODELS_FETCH=1 \
    OPENCODE_DISABLE_AUTOUPDATE=1 \
    OPENCODE_DISABLE_LSP_DOWNLOAD=1 \
    OPENCODE_DISABLE_PROJECT_CONFIG=1 \
    HOME=/home/bench \
    PATH=/home/bench/.local/bin:$PATH

# Всё выполняется непривилегированно: PID 1, exec и агент — пользователь bench.
USER bench

WORKDIR /work
ENTRYPOINT ["/usr/local/bin/bench-entry"]
CMD ["sleep", "infinity"]
