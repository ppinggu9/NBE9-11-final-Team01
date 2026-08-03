/**
 * drift-chunk.js — DriftReconciliation MGET 청크 크기 튜닝 (Redis 싱글스레드 버스트 최적화)
 *
 * 목적: 청크 크기별로 ① 드리프트 스윕 시간 ② 스윕 중 동시 주문 지연 을 측정 → 최적 청크 확정.
 *       (설계 문서 미결 과제: "청크 500 적정값은 부하테스트로 확정")
 *
 * 전제:
 *   - zone 대량 시딩 (LoadTestDataInitializer ZONE_COUNT=2000 등).
 *   - 청크값은 앱 기동 env로 주입: RECONCILE_DRIFTCHUNKSIZE=100/250/500/1000/2000
 *   - admin 드리프트 트리거 엔드포인트(POST /admin/drift, @Profile("loadtest")) + durationMs 반환.
 *
 * 실행 (청크값 바꿔가며 반복):
 *   RECONCILE_DRIFTCHUNKSIZE=500 ./loadtest/run.sh drift-chunk
 *
 * env: EVENT_ID·ZONE_DB_IDS(자동) · LOAD_RATE(100) · DURATION(120s) · DRIFT_EVERY(10s)
 */
import { check, sleep } from 'k6'
import { Counter, Trend } from 'k6/metrics'
import http from 'k6/http'
import { signUp, login, placeOrder, BASE_URL, JSON_HEADERS } from '../utils/helpers.js'

// ── 환경변수 ─────────────────────────────────────────────────
const EVENT_ID = __ENV.EVENT_ID
const ZONE_DB_IDS = (
    __ENV.ZONE_DB_IDS
        ? __ENV.ZONE_DB_IDS.split(',').map((s) => parseInt(s, 10))
        : [parseInt(__ENV.REDIS_STOCK_KEY.split(':')[1], 10)]
).filter((n) => !isNaN(n))
function pickZone() { return ZONE_DB_IDS[(__VU + __ITER) % ZONE_DB_IDS.length] }

const LOAD_RATE = parseInt(__ENV.LOAD_RATE || '100', 10)
const DURATION = __ENV.DURATION || '120s'
const DRIFT_EVERY = parseInt(__ENV.DRIFT_EVERY || '10', 10) // 드리프트 트리거 주기(초)
const CHUNK = __ENV.RECONCILE_DRIFTCHUNKSIZE || '?'         // 로그 라벨용(측정엔 미사용)

// ── 측정 지표 ────────────────────────────────────────────────
const driftDuration = new Trend('drift_duration_ms', true) // ★ 서버측 드리프트 스윕 시간
const orderLatency = new Trend('order_latency_ms', true)   // ★ 동시 주문 지연(버스트 영향)
const driftRuns = new Counter('drift_runs')

export const options = {
    scenarios: {
        // (A) 동시 주문 부하 — 드리프트 MGET 버스트가 방해할 트래픽. 이 지연을 측정.
        background_load: {
            executor: 'constant-arrival-rate',
            rate: LOAD_RATE, timeUnit: '1s', duration: DURATION,
            preAllocatedVUs: 100, maxVUs: 400, exec: 'backgroundLoad',
        },
        // (B) 드리프트 드릴러 — 주기적으로 admin 트리거, durationMs 기록(단일 VU).
        driller: {
            executor: 'constant-vus',
            vus: 1, duration: DURATION, exec: 'driller',
        },
    },
    noCookiesReset: true,
    // 특성화용 — 하드 게이트 아님. 청크별 비교가 목적.
}

const users = JSON.parse(open('../seed/users.json'))
let authed = false
let adminReady = false

export function setup() { users.forEach((u) => signUp(u.email, u.password)) }

// ── (A) 동시 주문: Redis stock 차감 → 드리프트 MGET과 싱글스레드 경합 ──
export function backgroundLoad() {
    if (!authed) {
        const u = users[(__VU - 1) % users.length]
        login(u.email, u.password)
        authed = true
    }
    const res = placeOrder(EVENT_ID, pickZone())
    orderLatency.add(res.timings.duration) // 이 지연이 드리프트 버스트에 영향받는다
    if (res.status === 401) authed = false
    // 결제 폴링 없음 — 순수 인게스트 부하로 Redis 경합만 발생
}

// ── (B) 드리프트 트리거: durationMs + report 기록 ──
export function driller() {
    if (!adminReady) { login('admin@snaptix.kr', 'Admin1234!'); adminReady = true }

    const res = http.post(`${BASE_URL}/api/v1/admin/drift`, null, { headers: JSON_HEADERS })
    if (check(res, { 'drift 200': (r) => r.status === 200 })) {
        const body = res.json()
        driftDuration.add(body.durationMs)
        driftRuns.add(1)
        const r = body.report
        console.warn(
            `[DRIFT] chunk=${CHUNK} durationMs=${body.durationMs} ` +
            `fixed=${r.fixed} skipped=${r.skipped} unchanged=${r.unchanged} failed=${r.failed}`,
        )
    } else {
        console.error(`[DRIFT] 실패 status=${res.status} body=${res.body}`)
    }
    sleep(DRIFT_EVERY)
}
