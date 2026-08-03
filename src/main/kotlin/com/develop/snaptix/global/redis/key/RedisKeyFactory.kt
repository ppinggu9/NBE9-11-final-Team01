// 위치: src/main/kotlin/com/develop/snaptix/global/redis/key/RedisKeyFactory.kt
package com.develop.snaptix.global.redis.key

import org.springframework.stereotype.Component
import java.util.UUID

/**
 * 모든 Redis 키 조립의 단일 진실 소스(SSOT). (Redis 키 명세서 v3.1)
 *
 * 키에 들어가는 식별자의 타입을 시그니처로 강제하여 호출부의 ID 타입 혼동을 방지한다.
 * - `zoneId`/`userId`: 내부 PK([Long]) — DB JOIN/FK 전용, 외부 미노출
 * - `eventPublicId`/`orderId`: [UUID] — `events.public_id` 또는 `reservations.order_id`
 *
 * 주의: 클라이언트 API의 `public_id`(UUID)를 내부 PK([Long])로 변환하는 책임은
 * **서비스 계층**에 있다. 본 팩토리는 변환하지 않고 이미 올바른 타입을 받는다.
 */
@Component
class RedisKeyFactory {
    // ── 재고/점유: zoneId = 내부 PK(Long) ──────────────────────────────

    /** 실시간 잔여 좌석. 집계 키(`stock:{eventId}`)는 만들지 않는다(Story 1.1). */
    fun stock(zoneId: Long): String = "ZONE:$zoneId:stock"

    /** 차감 완료 orderId 집합(워커 멱등). */
    fun claimed(zoneId: Long): String = "ZONE:$zoneId:claimed"

    // ── 주문 단위: orderId = UUID(노출) ────────────────────────────────

    /** 구매 선점/결제 타임아웃 홀드. */
    fun orderHold(orderId: UUID): String = "ORDER_HOLD:$orderId"

    /** 다중 서버 SSE 라우팅 채널. */
    fun sseOrder(orderId: UUID): String = "sse:order:$orderId"

    /** Webhook 멱등 가드. */
    fun webhookProcessed(orderId: UUID): String = "webhook:processed:$orderId"

    /** 결제 승인 이중 클릭 가드. */
    fun paymentApprove(orderId: UUID): String = "payment:approve:$orderId"

    /** PENDING 단계(예약 행 생성 전) 소유권 검증. */
    fun orderOwner(orderId: UUID): String = "order:owner:$orderId"

    // ── 유저×이벤트: userId = 내부 PK(Long), eventId = public_id(UUID) ──

    /** 멱등 키. 값=orderId, zoneId는 키에 포함하지 않음(1인 1이벤트 1매). */
    fun idempotency(
        userId: Long,
        eventPublicId: UUID,
    ): String = "idempotency:order:$userId:$eventPublicId"

    /** 처리 대기(PENDING) 상태 보정. */
    fun orderPending(
        userId: Long,
        eventPublicId: UUID,
    ): String = "order:pending:$userId:$eventPublicId"

    // ── Rate limit: 호출부가 정의한 임의의 키(IP 문자열 또는 "user:{userId}" 등), 초/분 두 윈도우 ──
    // 이전에는 IP 전용이었으나, 공유 IP(NAT/부하테스트)에서 사용자별 오탐 429를 막기 위해
    // 사용자 기준 한도를 병행 도입하면서 범용 문자열 키로 확장했다(RateLimitRedisGateway 참고).
    fun rateLimitSecond(key: String): String = "rate_limit:$key:sec"

    fun rateLimitMinute(key: String): String = "rate_limit:$key:min"

    // ── public_id(UUID) 키잉 ───────────────────────────────────────────

    /** 주문 큐 Stream. */
    fun queueOrder(eventPublicId: UUID): String = "queue:order:$eventPublicId"

    /** 이벤트 메타데이터 Cache-Aside. */
    fun eventInfo(eventPublicId: UUID): String = "event:info:$eventPublicId"

    /** 데이터 생존 표식(카나리). TTL 없음 — Redis가 데이터를 잃으면 함께 사라진다. */
    fun canary(): String = "snaptix:redis:canary"
}
