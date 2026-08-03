/**
 * cleanup.js — 이벤트 CLOSED 시 Redis 키 정리 검증 (부하 중 CLOSED 경쟁)
 *
 * 검증: 부하 중 CLOSED에도 ① 스트림 가드로 in-flight 보존 ② 즉시 키 삭제
 *       ③ 정착 후 고아 0 ④ CLOSED 후 지연주문 거부. k6가 Redis에 직접 붙어 3시점 스냅샷.
 *
 * 위치: loadtest/scenarios/cleanup.js  (상대 import 때문에 반드시 여기)
 * env : EVENT_ID·ZONE_DB_IDS(자동) · LOAD_RATE(20) · CLOSE_AFTER(40) · SETTLE(30) · DURATION(120s)
 *       REDIS_ADDR(redis://localhost:6380) · CONSUMER_GROUP(order-workers)
 * 참고: 첫 실행 시 k6가 k6/x/redis 번들 커스텀 바이너리를 자동 프로비저닝.
 */
import { check, sleep } from 'k6'
import { Counter } from 'k6/metrics'
import http from 'k6/http'
import redis from 'k6/x/redis'
import { signUp, login, placeOrder, BASE_URL, JSON_HEADERS } from '../utils/helpers.js'

// ── 환경변수 ─────────────────────────────────────────────────
const EVENT_ID = __ENV.EVENT_ID
const ZONE_DB_IDS = (
    __ENV.ZONE_DB_IDS
        ? __ENV.ZONE_DB_IDS.split(',').map((s) => parseInt(s, 10))
        : [parseInt(__ENV.REDIS_STOCK_KEY.split(':')[1], 10)]
).filter((n) => !isNaN(n))
function pickZone() { return ZONE_DB_IDS[(__VU + __ITER) % ZONE_DB_IDS.length] }

const LOAD_RATE = parseInt(__ENV.LOAD_RATE || '20', 10)
const DURATION = __ENV.DURATION || '120s'
const CLOSE_AFTER = parseInt(__ENV.CLOSE_AFTER || '40', 10) // CLOSED 주입까지 대기(초)
const SETTLE = parseInt(__ENV.SETTLE || '30', 10)          // 정착(드레인+스윕) 대기(초)
const CONSUMER_GROUP = __ENV.CONSUMER_GROUP || 'order-workers'

// ── Redis 클라이언트 (init 컨텍스트에서 1회 생성) ──────────────
const redisClient = new redis.Client(__ENV.REDIS_ADDR || 'redis://localhost:6380')

// ── 메트릭 ───────────────────────────────────────────────────
const closedOk = new Counter('event_closed_ok')
const lateOrders = new Counter('orders_after_closed')
const lateRejected = new Counter('rejected_after_closed')

export const options = {
    scenarios: {
        background_load: {
            executor: 'constant-arrival-rate',
            rate: LOAD_RATE, timeUnit: '1s', duration: DURATION,
            preAllocatedVUs: 50, maxVUs: 200, exec: 'backgroundLoad',
        },
        closer: {
            executor: 'per-vu-iterations',
            vus: 1, iterations: 1, maxDuration: DURATION, exec: 'closer',
        },
    },
    noCookiesReset: true,
}

const users = JSON.parse(open('../seed/users.json'))
let authed = false

export function setup() { users.forEach((u) => signUp(u.email, u.password)) }

// ── Redis 키 스냅샷 (k6가 직접 조회) ──────────────────────────
async function snapshot(label) {
    const info = await redisClient.exists(`event:info:${EVENT_ID}`) // 1/0
    let stock = 0
    let claimed = 0
    for (const z of ZONE_DB_IDS) {
        stock += await redisClient.exists(`ZONE:${z}:stock`)
        claimed += await redisClient.exists(`ZONE:${z}:claimed`)
    }
    const xlen = await redisClient.sendCommand('XLEN', `queue:order:${EVENT_ID}`)
    let pel = 'n/a'
    try {
        const xp = await redisClient.sendCommand('XPENDING', `queue:order:${EVENT_ID}`, CONSUMER_GROUP)
        pel = Array.isArray(xp) ? xp[0] : xp // XPENDING 요약 첫 값 = pending 개수
    } catch (e) {
        /* 그룹/스트림 없음 → n/a */
    }
    console.warn(
        `[SNAP:${label}] event:info=${info} stock=${stock}/${ZONE_DB_IDS.length} ` +
        `claimed=${claimed}/${ZONE_DB_IDS.length} streamXLEN=${xlen} PEL=${pel}`,
    )
}

// ── 배경 부하: 스트림·PEL·stock 채움 (결제 안 함 → PENDING/PEL 유지) ──
export function backgroundLoad() {
    if (!authed) {
        const u = users[(__VU - 1) % users.length]
        login(u.email, u.password)
        authed = true
    }
    const res = placeOrder(EVENT_ID, pickZone())
    if (res.status === 401) { authed = false; return }
    sleep(0.5) // 202/429/409/503 모두 정상 — in-flight 생성이 목적
}

// ── CLOSED 트리거 + 3시점 스냅샷 + 지연주문 거부 확인 ──────────
export async function closer() {
    sleep(CLOSE_AFTER)
    await snapshot('BEFORE_CLOSED') // 모든 키 존재 + 스트림 채워짐 기대

    login('admin@snaptix.kr', 'Admin1234!') // 이 VU 전용 쿠키 = admin
    const res = http.patch(
        `${BASE_URL}/api/v1/admin/events/${EVENT_ID}/status`,
        JSON.stringify({ status: 'CLOSED' }),
        { headers: JSON_HEADERS },
    )
    check(res, { 'CLOSED 전이 200': (r) => r.status === 200 })
    if (res.status === 200) {
        closedOk.add(1)
        console.warn(`[CLOSER] event CLOSED @${Date.now()}`)
    } else {
        console.error(`[CLOSER] CLOSED 실패 status=${res.status} body=${res.body}`)
    }

    sleep(1)
    await snapshot('AFTER_CLOSED') // info/stock/claimed 삭제? 스트림 보존?(PEL) — 가드 검증

    // CLOSED 후 지연 주문 → 전부 거부 + 키 미재생성 기대
    for (let i = 0; i < 10; i++) {
        const r = placeOrder(EVENT_ID, pickZone())
        lateOrders.add(1)
        if (r.status !== 202) lateRejected.add(1)
        sleep(0.3)
    }

    sleep(SETTLE)
    await snapshot('AFTER_SETTLE') // streamXLEN·PEL 0 이어야 고아 없음 — 최종 판정
}
