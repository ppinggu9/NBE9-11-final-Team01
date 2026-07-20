/**
 * 공유 임계값(Thresholds) 상수.
 * order-load.js / sse-reconnect.js 양쪽에서 import 해 사용한다.
 *
 * 기준 근거:
 *  - http_req_failed   : 전체 요청의 1% 미만 실패 → 정상
 *  - http_req_duration : p(95) < 300ms (인제스트는 202로 즉시 반환하므로 엄격)
 *                        p(99) < 600ms
 *  - oversell_errors   : 재고 음수(오버셀) 0건 → 절대 조건
 *  - backpressure_hit  : 429 응답 비율 20% 미만 → 과도한 백프레셔 감지
 */
export const thresholds = {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<300', 'p(99)<600'],
    oversell_errors: ['count<1'],
    backpressure_hit: ['rate<0.2'],
}
