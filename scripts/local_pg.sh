#!/usr/bin/env bash
# 无 Docker 环境下的本地 PostgreSQL 15 便携实例管理（免 root）。
# 用法:
#   ./scripts/local_pg.sh start   # 下载/解压二进制、initdb、启动 5433、建 patrol 库
#   ./scripts/local_pg.sh stop    # 停库
#   ./scripts/local_pg.sh logs    # 看数据库日志
# 数据目录: $HOME/.local/pgdata15 ; 二进制: $HOME/.local/pg15
set -euo pipefail

PG_VERSION="15.20-1~20261124.0506.g5e7f7f5ec96.pgdg12+1"
PGDG="http://apt.postgresql.org/pub/repos/apt/pool/main/p/postgresql-15"
BASE="${HOME}/.local"
PGROOT="${BASE}/pg15"
PGDATA="${BASE}/pgdata15"
PGBIN="${PGROOT}/usr/lib/postgresql/15/bin"
PORT=5433
DB=patrol

start() {
  if [[ ! -x "${PGBIN}/postgres" ]]; then
    mkdir -p "${BASE}" /tmp/pgdebs
    echo "[local_pg] 下载 PostgreSQL ${PG_VERSION} 便携包..."
    curl -fL --retry 3 -o /tmp/pgdebs/server.deb \
      "${PGDG}/postgresql-15_${PG_VERSION}_amd64.deb"
    curl -fL --retry 3 -o /tmp/pgdebs/client.deb \
      "${PGDG}/postgresql-client-15_${PG_VERSION}_amd64.deb"
    mkdir -p "${PGROOT}"
    dpkg-deb -x /tmp/pgdebs/server.deb "${PGROOT}"
    dpkg-deb -x /tmp/pgdebs/client.deb "${PGROOT}"
  fi
  if [[ ! -d "${PGDATA}" ]]; then
    echo "[local_pg] initdb -> ${PGDATA}"
    "${PGBIN}/initdb" -D "${PGDATA}" -U postgres --auth=trust \
      --no-locale --encoding=UTF8
    cat >> "${PGDATA}/postgresql.conf" <<EOF
port = ${PORT}
listen_addresses = '127.0.0.1'
unix_socket_directories = '/tmp'
deadlock_timeout = '200ms'
lock_timeout = '10s'
log_lock_waits = on
EOF
  fi
  if ! "${PGBIN}/pg_isready" -h 127.0.0.1 -p "${PORT}" -U postgres >/dev/null 2>&1; then
    echo "[local_pg] 启动实例 (port ${PORT})"
    "${PGBIN}/pg_ctl" -D "${PGDATA}" -l "${BASE}/pg15.log" start
    for _ in $(seq 1 30); do
      "${PGBIN}/pg_isready" -h 127.0.0.1 -p "${PORT}" -U postgres >/dev/null 2>&1 && break
      sleep 0.5
    done
  fi
  "${PGBIN}/psql" -h 127.0.0.1 -p "${PORT}" -U postgres -tc \
    "SELECT 1 FROM pg_database WHERE datname='${DB}'" | grep -q 1 \
    || "${PGBIN}/createdb" -h 127.0.0.1 -p "${PORT}" -U postgres "${DB}"
  echo "[local_pg] ready: postgresql://postgres@127.0.0.1:${PORT}/${DB}"
}

stop() {
  [[ -x "${PGBIN}/pg_ctl" ]] && "${PGBIN}/pg_ctl" -D "${PGDATA}" stop || true
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  logs) tail -f "${BASE}/pg15.log" ;;
  *) echo "usage: $0 {start|stop|logs}"; exit 1 ;;
esac
