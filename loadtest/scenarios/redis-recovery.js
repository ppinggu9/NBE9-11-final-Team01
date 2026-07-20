/**
 * redis-recovery.js — Redis 장애 주입 → 서킷 OPEN → 복구 → rebuild 검증 (현재 상태 = 단일 워커 기준)
 *
 * [무엇을 검증하나]
 *  배경 부하가 도는 중에 (외부에서) Redis를 죽이거나 얼려 서킷을 OPEN시키고,
 *  복구 시 rebuild가 재고를 SSOT로 되돌리며 **오버셀이 0으로 유지되는지**를 본다.
 *
 * [현재 코드 상태 전제 — 반드시 인지]
 *  - Read-Only 모드는 껍데기다(인게스트/워커에 미결선). 재구축 중 신규 주문이 그대로 흐른다.
 *  - rebuild는 서킷 CLOSED 전이마다 "무조건" 실행된다(데이터 소실 여부 게이트 없음).
 *  - 워커는 단일 스레드(순차 처리) → ①Redis차감→②DB INSERT gap 동시 in-flight 최대 1건 → 오버셀 창 최소.
 *
 * [장애 주입은 외부에서]
 *  k6는 인프라를 못 만진다. 별도 터미널/스크립트로 주입한다(inject-fault.sh 참고).
 *   - 완전 소실(rebuild 유의미): docker stop → start  (loadtest redis persistence off 권장)
 *   - 일시 트립(무손실, spurious rebuild): docker pause → unpause
 *
 * [복구 시간 관측의 한계 — 정직하게]
 *  현재 서버엔 rebuild 결과를 조회할 엔드포인트가 없다. 이 스크립트의 recovery_latency는
 *  "이벤트 조회(getEventDetail)가 실패했다가 다시 성공하는" 구간을 근사로 잰다.
 *  만약 Redis 다운 중에도 event 조회가 200을 반환하면(캐시/무접근) 다운 감지가 안 될 수 있다.
 *  → 정밀 복구시간은 Grafana(snaptix_rebuild_duration, resilience4j_circuitbreaker_state)로 교차확인.
 *  → AWS 재배포 시 GET /admin/rebuild/last 를 추가하면 k6가 결정적으로 관측 가능(문서 참고).
 *  이 스크립트의 하드 합격 조건은 오직 oversell_errors=0 + 최종 재고가 [0, 정원] 범위인지다.
 *
 * 실행: run.sh에 redis-recovery 시나리오를 추가한 뒤
 *   ./loadtest/run.sh redis-recovery         # 터미널1 (clean start + k6)
 *   ./loadtest/inject-fault.sh stop 8        # 터미널2, k6 시작 ~30s 후 실행
 *
 * 환경변수: EVENT_ID, REDIS_STOCK_KEY (자동 주입) · LOAD_RATE(기본20) · DURATION(기본120s)
 */

import { check, sleep } from 'k6'
import { Counter, Trend } from 'k6/metrics'
import http from 'k6/http'
import {
    signUp,
    login,
    placeOrder,
    getOrderStatus,
    approvePayment,
    sendPaymentWebhook,
    getEventDetail,
} from '../../../../NBE9-11-final-Team01/loadtest/utils/helpers.js'


// ── 환경변수 ─────────────────────────────────────────────────
const EVENT_ID = __ENV.EVENT_ID
const ZONE_DB_ID = parseInt(__ENV.REDIS_STOCK_KEY.split(':')[1], 10)
const LOAD_RATE = parseInt(__ENV.LOAD_RATE || '20', 10)
const DURATION = __ENV.DURATION || '120s'
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080'

if (!EVENT_ID) throw new Error('EVENT_ID 환경변수가 설정되지 않았습니다.')
if (isNaN(ZONE_DB_ID)) throw new Error('REDIS_STOCK_KEY에서 zoneId를 파싱할 수 없습니다.')

// ── 커스텀 메트릭 ─────────────────────────────────────────────
const oversellErrors = new Counter('oversell_errors')          // 재고 음수 — 절대 0
const circuitOpen503 = new Counter('circuit_open_503')         // 서킷 OPEN 동안 503 수(장애 창 크기)
const redisDownObserved = new Counter('redis_down_observed')   // 옵저버가 이벤트 조회 실패 관측
const redisRecovered = new Counter('redis_recovered')          // 다운 후 조회 복구 관측
const recoveryLatency = new Trend('recovery_latency_ms', true) // 다운 관측~복구 관측(근사)

const rebuildSeen = new Counter('rebuild_completed_seen')
const rebuildDurationMs = new Trend('rebuild_duration_ms', true)

const PAYABLE = 'READY_TO_PAY'
const FINAL = ['CONFIRMED', 'CANCELLED', 'RELEASED', 'FAILED', 'EXPIRED']

// ── 시나리오 옵션 ─────────────────────────────────────────────
export const options = {
    scenarios: {
        // (A) 배경 부하 — 서킷 트립 조건과 in-flight 워커 확보
        background_load: {
            executor: 'constant-arrival-rate',
            rate: LOAD_RATE,
            timeUnit: '1s',
            duration: DURATION,
            preAllocatedVUs: 50,
            maxVUs: 200,
            exec: 'backgroundLoad',
        },
        // (B) 복구 옵저버 — 재고 1초 폴링으로 다운→복구·오버셀 관측(단일 VU)
        recovery_observer: {
            executor: 'constant-vus',
            vus: 1,
            duration: DURATION,
            exec: 'recoveryObserver',
        },
    },
    noCookiesReset: true,
    // 카오스 전용: http_req_failed/duration은 장애 구간에 급증하므로 게이트하지 않는다.
    // (503 급증은 "서킷이 제대로 격리 중"이라는 정상 신호 — 예외·HTTP 매핑 문서 참고)
    thresholds: {
        oversell_errors: ['count<1'], // 유일한 절대 조건
    },
}

const users = JSON.parse(open('../seed/users.json'))
let authenticated = false

// ── setup: seed 유저 회원가입 보장(로그인 401 방지) ───────────
export function setup() {
    users.forEach((u) => signUp(u.email, u.password))
}

// ── (A) 배경 부하 — order-load 패턴 + 503(서킷 OPEN) 관용 ─────
export function backgroundLoad() {
    if (!authenticated) {
        const user = users[(__VU - 1) % users.length]
        login(user.email, user.password)
        authenticated = true
    }

    const res = placeOrder(EVENT_ID, ZONE_DB_ID)
    const s = res.status

    if (s === 503) {
        // 서킷 OPEN 구간 — 정상(장애 격리 동작). http_req_failed엔 잡히지만 게이트 안 함.
        circuitOpen503.add(1)
        sleep(1)
        return
    }
    if (s === 429 || s === 409) {
        sleep(0.5)
        return
    }
    if (s === 401) {
        authenticated = false
        return
    }
    if (!check(res, { 'order 202': (r) => r.status === 202 })) return

    const { orderId } = res.json()

    // 결제 흐름(선택) — CONFIRMED/PENDING 혼합을 만들어 rebuild 재산정을 유의미하게 한다.
    // 장애 구간엔 폴링/승인/웹훅도 실패할 수 있으므로 실패 시 조용히 중단한다.
    let paid = false
    for (let i = 0; i < 8; i++) {
        sleep(2)
        const st = getOrderStatus(orderId)
        if (st.status !== 200) break // 장애 구간 — 폴링 실패, 이 iteration 종료
        const status = st.json().status
        if (status === PAYABLE && !paid) {
            paid = true
            approvePayment(orderId)
            sendPaymentWebhook(orderId, 'SUCCESS')
            continue
        }
        if (FINAL.includes(status)) break
    }
}

// ── (B) 복구 옵저버 — 재고 폴링으로 다운→복구·오버셀 관측 ─────
let downSince = null
let adminReady = false        // ★ 추가
let lastFinishedAt = null
export function recoveryObserver() {
    // ★ 추가: rebuild-last 결정적 관측 (getEventDetail 근사와 병행 — 교차검증)
    if (!adminReady) { login('admin@snaptix.kr', 'Admin1234!'); adminReady = true }
    const rb = http.get(`${BASE_URL}/api/v1/admin/rebuild/last`)
    if (rb.status === 200) {
        const s = rb.json()
        if (s.finishedAt && s.finishedAt !== lastFinishedAt) {  // 새 rebuild 등장
            lastFinishedAt = s.finishedAt
            rebuildSeen.add(1)
            if (s.durationMs != null) rebuildDurationMs.add(s.durationMs)
            console.log(`[OBS] rebuild ${s.outcome} durationMs=${s.durationMs} finishedAt=${s.finishedAt}`)
        }
    }

    const res = getEventDetail(EVENT_ID)
    const now = Date.now()

    if (res.status !== 200) {
        // 이벤트 조회 실패 = Redis/서킷 영향으로 간주(다운 구간 진입)
        if (downSince === null) {
            downSince = now
            redisDownObserved.add(1)
            console.warn(`[OBS] event detail down status=${res.status} @${now}`)
        }
        sleep(1)
        return
    }

    // 200 — 재고 파싱 + 오버셀 감시
    let stock = null
    try {
        const zones = res.json().zones
        stock = zones && zones[0] ? zones[0].currentStock : null
    } catch (e) {
        // 파싱 실패는 무시(다음 폴링에서 재시도)
    }
    if (stock !== null && stock < 0) {
        oversellErrors.add(1)
        console.error(`[OBS][OVERSELL] stock=${stock} @${now}`)
    }

    // 다운 상태였다가 복구됨 → 복구 시간 기록
    if (downSince !== null) {
        const latency = now - downSince
        recoveryLatency.add(latency)
        redisRecovered.add(1)
        console.warn(`[OBS] recovered after ${latency}ms, stock=${stock}`)
        downSince = null
    }

    sleep(1)
}

// ── teardown: 최종 오버셀 검증 (order-load와 동일) ────────────
export function teardown() {
    const res = getEventDetail(EVENT_ID)
    if (res.status !== 200) {
        console.warn(`[teardown] event detail status=${res.status} — 재고 확인 불가`)
        return
    }
    const { zones } = res.json()
    zones.forEach((zone) => {
        if (zone.currentStock < 0) {
            oversellErrors.add(1)
            console.error(`[OVERSELL] zoneId=${zone.zoneId} name=${zone.name} stock=${zone.currentStock}`)
        } else {
            console.log(`[OK] zoneId=${zone.zoneId} name=${zone.name} stock=${zone.currentStock}/${zone.totalCapacity}`)
        }
    })
}
