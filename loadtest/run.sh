#!/usr/bin/env bash
# loadtest/run.sh — 매 실행 전 Redis/앱을 초기화한 뒤 k6를 실행하는 래퍼 스크립트
#
# 사용법:
#   ./loadtest/run.sh [시나리오] [옵션]
#   ./loadtest/run.sh stop              # run.sh가 띄운 앱 프로세스 종료
#
# 시나리오 (기본값: order-load):
#   order-load       주문 인제스트 부하 테스트
#   sse-reconnect    SSE 연결·재연결 내구성 테스트
#
# 옵션:
#   --reset      (기본값) Redis FLUSHALL + 앱 재기동 후 실행한다.
#                이전 테스트 실행/정리 여부와 무관하게 항상 새 이벤트·재고·
#                멱등 상태에서 시작한다.
#   --no-reset   초기화를 건너뛰고 기존 loadtest/seed/.env를 그대로 사용한다.
#                (앱을 별도 터미널에서 직접 기동해 관리하는 기존 방식과 호환)
#
# 왜 기본값이 --reset인가:
#   order-load / sse-reconnect는 동일한 유저 풀(load-user-1..200)과 동일한
#   EVENT_ID/ZONE_ID를 공유한다. 앱을 재기동하지 않고 연달아 실행하면
#     1) 재고(TOTAL_CAPACITY=100)가 이미 소진돼 있거나
#     2) 같은 유저가 같은 이벤트에 이미 주문을 넣어 멱등 충돌(409)만
#        반복 관측되는 등, 두 번째 실행이 실제로는 거의 검증을 하지 못한
#        채로 "통과"할 수 있다.
#   EventController에는 이벤트를 생성하는 HTTP API가 없고(GET만 존재),
#   LoadTestDataInitializer가 앱 기동 시에만 새 이벤트/구역/재고를 시딩하므로
#   "항상 깨끗한 상태"를 보장하는 유일한 방법은 앱을 재기동하는 것이다.
#   Redis는 컨테이너 재기동(docker restart) 대신 FLUSHALL로 명시적으로
#   비운다 — RDB 스냅샷이 남아있으면 restart만으로는 데이터가 되살아날 수
#   있기 때문이다. MySQL은 건드리지 않는다(이벤트/구역이 매번 새 ID로
#   생성되므로 이전 데이터가 남아있어도 충돌하지 않는다 — 다만 시간이
#   지나면 누적되므로 README의 "주기적 정리" 항목을 참고).
#
# 프로젝트 루트에서 실행하세요.

set -euo pipefail

# ── 설정 ────────────────────────────────────────────────────────────────────
COMPOSE_FILE="docker-compose.loadtest.yml"
REDIS_CONTAINER="${REDIS_CONTAINER:-snaptix-redis-loadtest}"
APP_PORT="${APP_PORT:-8080}"
ENV_FILE="loadtest/seed/.env"

# loadtest/results/ 는 .gitignore에 이미 포함돼 있어 여기에 실행 산출물을 둔다.
RUN_DIR="loadtest/results/.run"
APP_LOG_FILE="$RUN_DIR/app.log"
APP_PID_FILE="$RUN_DIR/app.pid"

SEED_MARKER="시드 완료"
SEED_TIMEOUT_SEC="${SEED_TIMEOUT_SEC:-180}"     # 최초 실행(콜드 빌드)은 더 걸릴 수 있음 — 필요 시 env로 조정
HEALTH_TIMEOUT_SEC="${HEALTH_TIMEOUT_SEC:-30}"

# ── 인자 파싱 ────────────────────────────────────────────────────────────────
SCENARIO="order-load"
DO_RESET=true

for arg in "$@"; do
  case "$arg" in
    --reset) DO_RESET=true ;;
    --no-reset) DO_RESET=false ;;
    stop|order-load|sse-reconnect|redis-recovery|cleanup|drift-chunk) SCENARIO="$arg" ;;
    *)
      echo "[오류] 알 수 없는 인자: $arg"
      echo "       사용법: ./loadtest/run.sh [order-load|sse-reconnect|stop] [--reset|--no-reset]"
      exit 1
      ;;
  esac
done

mkdir -p "$RUN_DIR"

# ── 유틸 ────────────────────────────────────────────────────────────────────

container_running() {
  [[ "$(docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null || echo false)" == "true" ]]
}

port_pids() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti tcp:"$APP_PORT" 2>/dev/null || true
  fi
}

# run.sh가 띄운(PID 파일) 인스턴스는 물론, 별도 터미널에서 수동으로 띄운
# 인스턴스까지 포트 기준으로 함께 정리한다.
stop_app() {
  local touched=false

  if [[ -f "$APP_PID_FILE" ]]; then
    local pid
    pid="$(cat "$APP_PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      echo "[reset] 앱 프로세스 종료 요청 (PID=$pid)"
      kill "$pid" 2>/dev/null || true
      touched=true
    fi
    rm -f "$APP_PID_FILE"
  fi

  if ! command -v lsof >/dev/null 2>&1; then
    echo "[reset] lsof가 없어 :$APP_PORT 포트 점유 프로세스는 자동으로 정리하지 못합니다."
    echo "        기존에 수동으로 띄운 앱이 남아있다면 직접 종료해주세요."
  else
    local pids
    pids="$(port_pids)"
    if [[ -n "$pids" ]]; then
      echo "[reset] :$APP_PORT 포트를 점유한 프로세스 정리: $pids"
      echo "$pids" | xargs -r kill 2>/dev/null || true
      touched=true
    fi

    if [[ "$touched" == "true" ]]; then
      for _ in $(seq 1 20); do
        [[ -z "$(port_pids)" ]] && break
        sleep 0.5
      done
      local remaining
      remaining="$(port_pids)"
      if [[ -n "$remaining" ]]; then
        echo "[reset] 정상 종료되지 않아 강제 종료(-9)합니다: $remaining"
        echo "$remaining" | xargs -r kill -9 2>/dev/null || true
      fi
    fi
  fi
}


# LoadTestDataInitializer.writeSeedEnv()는 기존 .env가 있을 때만 그 안의
# BASE_URL 등을 보존한 채 EVENT_ID/ZONE_ID/REDIS_STOCK_KEY만 덮어쓴다.
# .env가 아예 없는 최초 실행(예: docker compose up 직후 최초 부팅)이면
# BASE_URL 없이 3줄만 생성되어 이후 source 시 "unbound variable"로 죽는다.
# 그래서 앱을 띄우기 전에 템플릿에서 .env를 미리 만들어둔다.
ensure_env_template() {
  if [[ -f "$ENV_FILE" ]]; then
    return
  fi
  if [[ -f "${ENV_FILE}.example" ]]; then
    echo "[reset] $ENV_FILE 이 없어 템플릿에서 생성합니다 (${ENV_FILE}.example → $ENV_FILE)"
    cp "${ENV_FILE}.example" "$ENV_FILE"
  else
    echo "[경고] ${ENV_FILE}.example 도 없습니다. BASE_URL 등 기본값이 비어있을 수 있습니다."
  fi
}

reset_redis() {
  if ! container_running "$REDIS_CONTAINER"; then
    echo "[오류] Redis 컨테이너($REDIS_CONTAINER)가 실행 중이 아닙니다."
    echo "       먼저 실행하세요: docker compose -f $COMPOSE_FILE up -d"
    exit 1
  fi
  echo "[reset] Redis 초기화 (FLUSHALL) — $REDIS_CONTAINER"
  docker exec "$REDIS_CONTAINER" redis-cli FLUSHALL >/dev/null
}

start_app() {
  echo "[reset] 앱 재기동 (loadtest 프로파일) — 새 이벤트/재고 시딩 대기 중..."
  : > "$APP_LOG_FILE"

  # --no-daemon: Gradle 데몬에 프로세스가 흡수되지 않도록 해 종료(kill) 시점을
  # 예측 가능하게 만든다. stop_app의 포트 기준 강제 종료가 최종 안전망이다.
  (SPRING_PROFILES_ACTIVE=loadtest ./gradlew --no-daemon bootRun >>"$APP_LOG_FILE" 2>&1) &
  echo $! > "$APP_PID_FILE"

  local waited=0
  while ! grep -q "$SEED_MARKER" "$APP_LOG_FILE" 2>/dev/null; do
    if ! kill -0 "$(cat "$APP_PID_FILE")" 2>/dev/null; then
      echo "[오류] 앱이 시딩 완료 전에 종료되었습니다. 로그를 확인하세요: $APP_LOG_FILE"
      tail -n 40 "$APP_LOG_FILE" || true
      exit 1
    fi
    if (( waited >= SEED_TIMEOUT_SEC )); then
      echo "[오류] ${SEED_TIMEOUT_SEC}초 내에 시드 완료를 확인하지 못했습니다. 로그: $APP_LOG_FILE"
      echo "       (최초 실행/콜드 빌드라면 SEED_TIMEOUT_SEC=300 ./loadtest/run.sh ... 로 늘려보세요)"
      tail -n 40 "$APP_LOG_FILE" || true
      exit 1
    fi
    sleep 1
    ((waited += 1)) || true
  done
  echo "[reset] 시드 완료 확인 (${waited}s) — $ENV_FILE 갱신됨"

  local health_waited=0
  until curl -sf "http://localhost:${APP_PORT}/actuator/health" 2>/dev/null | grep -q '"status":"UP"'; do
    if (( health_waited >= HEALTH_TIMEOUT_SEC )); then
      echo "[경고] 헬스체크(UP) 확인에 실패했지만 계속 진행합니다. (${APP_PORT}) 로그: $APP_LOG_FILE"
      break
    fi
    sleep 1
    ((health_waited += 1)) || true
  done
}

# ── stop 서브커맨드 ───────────────────────────────────────────────────────────
if [[ "$SCENARIO" == "stop" ]]; then
  stop_app
  echo "[run.sh] 앱 프로세스를 정리했습니다."
  exit 0
fi

# ── 시나리오 파일 존재 확인 ────────────────────────────────────────────────────
SCRIPT="loadtest/scenarios/${SCENARIO}.js"
if [[ ! -f "$SCRIPT" ]]; then
  echo "[오류] 시나리오 파일을 찾을 수 없습니다: $SCRIPT"
  echo "       사용 가능한 시나리오: order-load, sse-reconnect"
  exit 1
fi

# ── 초기화 ───────────────────────────────────────────────────────────────────
if [[ "$DO_RESET" == "true" ]]; then
  stop_app
  ensure_env_template
  reset_redis
  start_app
else
  echo "[run.sh] --no-reset: 초기화를 건너뜁니다. 이전 실행 상태가 남아있을 수 있습니다."
fi

if [[ ! -f "$ENV_FILE" ]]; then
  echo "[오류] $ENV_FILE 파일이 없습니다."
  echo "       SPRING_PROFILES_ACTIVE=loadtest ./gradlew bootRun 을 먼저 실행해 시드 데이터를 생성하거나"
  echo "       옵션 없이(기본값 --reset) 다시 실행해보세요."
  exit 1
fi

# .env 로드 (주석·빈 줄 제외)
set -a
# shellcheck source=/dev/null
source "$ENV_FILE"
set +a

# ensure_env_template로도 못 채운 경우를 대비한 최후 방어선 (예: .env.example 자체가 없던 경우)
BASE_URL="${BASE_URL:-http://localhost:${APP_PORT}}"

echo "[k6] 시나리오: $SCRIPT"
echo "[k6] BASE_URL=$BASE_URL | EVENT_ID=$EVENT_ID | REDIS_STOCK_KEY=$REDIS_STOCK_KEY"
echo "--------------------------------------"

k6 run "$SCRIPT"
