#!/bin/sh
# Entrypoint задачи. Работает непривилегированным пользователем bench (образ
# объявляет USER bench), поэтому никакие capabilities не требуются. Первым
# делом распаковывает архив дерева в /workspace/postgres, готовит конфиг
# OpenCode, ставит маркер готовности и остаётся живым. Bind mounts не
# используются.
set -eu

rm -f /work/ready
if [ -s /work/tree.tar.gz ]; then
    mkdir -p /workspace/postgres
    tar --no-same-owner -xzf /work/tree.tar.gz -C /workspace/postgres
    rm -f /work/tree.tar.gz
fi

mkdir -p /workspace/postgres /work /home/bench/.config/opencode
touch /work/ready

exec "$@"
