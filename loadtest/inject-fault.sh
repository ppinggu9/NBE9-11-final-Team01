#!/usr/bin/env bash
# loadtest/inject-fault.sh — 부하 중 Redis 장애를 주입해 서킷을 OPEN시키는 스크립트.
#
# k6(redis-recovery.js)가 도는 중, 별도 터미널에서 실행한다.
#
# 사용법:
#   ./loadtest/inject-fault.sh <mode> [downSeconds]
#     mode=stop  : docker stop → (downSeconds 후) start   — 완전 소실 재현(rebuild 유의미)
#                  ※ loadtest redis에 persistence off가 있어야 결정적으로 빈 Redis가 된다
#                    (docker-compose.loadtest.yml 의 redis-loadtest 에
#                     command: redis-server --save "" --appendonly no  추가 권장)
#     mode=pause : docker pause → (downSeconds 후) unpause — 무손실 일시 트립(spurious rebuild)
#   downSeconds  : 다운 유지 시간(기본 8초). 서킷 OPEN(실패 5건↑) + wait 10s를 넘기려면 8~15 권장.
#
# 예:
#   ./loadtest/inject-fault.sh stop 8
#   ./loadtest/inject-fault.sh pause 6

set -euo pipefail

MODE="${1:-}"
DOWN_FOR="${2:-8}"
REDIS_CONTAINER="${REDIS_CONTAINER:-snaptix-redis-loadtest}"

if [[ "$MODE" != "stop" && "$MODE" != "pause" ]]; then
  echo "[오류] mode는 stop 또는 pause 여야 합니다."
  echo "       사용법: ./loadtest/inject-fault.sh <stop|pause> [downSeconds]"
  exit 1
fi

if [[ "$(docker inspect -f '{{.State.Running}}' "$REDIS_CONTAINER" 2>/dev/null || echo false)" != "true" ]]; then
  echo "[오류] Redis 컨테이너($REDIS_CONTAINER)가 실행 중이 아닙니다."
  exit 1
fi

ts() { date '+%H:%M:%S'; }

if [[ "$MODE" == "stop" ]]; then
  echo "[$(ts)] [fault] docker stop $REDIS_CONTAINER (완전 소실 재현)"
  docker stop "$REDIS_CONTAINER" >/dev/null
  echo "[$(ts)] [fault] ${DOWN_FOR}s 대기 (서킷 OPEN 유도)..."
  sleep "$DOWN_FOR"
  echo "[$(ts)] [fault] docker start $REDIS_CONTAINER (복구 → 서킷 CLOSED → rebuild 예상)"
  docker start "$REDIS_CONTAINER" >/dev/null
else
  echo "[$(ts)] [fault] docker pause $REDIS_CONTAINER (무손실 일시 트립)"
  docker pause "$REDIS_CONTAINER" >/dev/null
  echo "[$(ts)] [fault] ${DOWN_FOR}s 대기 (서킷 OPEN 유도)..."
  sleep "$DOWN_FOR"
  echo "[$(ts)] [fault] docker unpause $REDIS_CONTAINER (데이터 그대로 → spurious rebuild 예상)"
  docker unpause "$REDIS_CONTAINER" >/dev/null
fi

echo "[$(ts)] [fault] 주입 완료. k6 요약(oversell_errors, recovery_latency)과 Grafana(rebuild/서킷)로 확인하세요."
