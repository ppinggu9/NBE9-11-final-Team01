package com.develop.snaptix.domain.reservation.service

import com.develop.snaptix.domain.auditlog.repository.AuditLogRepository
import com.develop.snaptix.domain.reservation.repository.DriftStockRepository
import com.develop.snaptix.domain.reservation.repository.ZoneExpected
import com.develop.snaptix.global.alert.model.AlertContext
import com.develop.snaptix.global.alert.model.AlertTrigger
import com.develop.snaptix.global.alert.service.AlertService
import com.develop.snaptix.global.aop.type.RedisAction
import com.develop.snaptix.global.redis.gateway.StockRedisGateway
import com.develop.snaptix.global.resilience.config.ReconcileProperties
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import org.springframework.stereotype.Service
import java.time.Instant

/**
 * 상시 재고 드리프트 정산. (작업 명세서 v2.1 §6 · Story 13.4 / S-13)
 *
 * 장애와 무관하게 집계 기준으로 Redis 재고를 SSOT 기대값과 비교한다.
 *  - 누수(`actual < expected`)  → 절대 SET 보정(correctStock) + `STOCK_DRIFT_FIX` 감사(best-effort).
 *  - 오버셀(`actual > expected`) → **자동 보정 안 함**, `STOCK_DRIFT_OVERSELL` 알림만(값 불변·감사 없음).
 *  - `stock` 키 부재 → skip (rebuild 가 생성 책임).
 *  - 동일 → 무동작.
 *
 * 읽기: [DriftStockRepository.aggregateExpectedStock] 단일 SELECT(JOIN+GROUP BY) 강한 스냅샷.
 * 적용: 결과를 [CHUNK_SIZE] 단위로 끊어 `getAll`(MGET)+보정 — Redis 과부하 방지(DB 읽기는 청크하지 않음).
 * 격리: 청크/zone 단위 예외를 잡아 `failed` 로 집계 후 계속(멱등·30분 재수렴으로 다음 런 재시도).
 */

@Service
class DriftReconciliationService(
    private val driftStockRepository: DriftStockRepository,
    private val stockRedisGateway: StockRedisGateway,
    private val auditLogRepository: AuditLogRepository,
    private val alertService: AlertService,
    private val reconcileProperties: ReconcileProperties,
) {
    private val logger = KotlinLogging.logger {}

    fun checkDrift(now: Instant): DriftReport {
        // 시계 1회 고정(계약 #0)
        val holdCutoff = now.minus(reconcileProperties.holdWindow)

        // (읽기) 단일 SELECT 강한 스냅샷 — 트랜잭션은 조회 반환과 함께 닫힌다.
        val plans = driftStockRepository.aggregateExpectedStock(holdCutoff)

        // (적용) Redis 부하 분산 — 청크 단위로 MGET 후 보정/알림.
        // chunked: "컬렉션을 n 개씩 잘라서 처리 단위로 나누는 함수"(버스 태우기)
        // => size 양수, 생성된 list는 바로 반환, 나머지값은 그대로 전송(%연산자처럼 연산)
        val acc = DriftReport.Accumulator() // 누적 집계기 생성,  toReport()로 누적
        plans.chunked(reconcileProperties.driftChunkSize).forEach { chunk -> applyChunk(chunk, acc) } // 참조만 전달, 상태 공유

        // acc 목적: 드리프트 실행 중 발생했던 결과를 참조 전달로 누적하고 리포트에 반환(상태 수집기)
        return acc.toReport().also { report ->
            logger.atInfo {
                message = "Stock drift reconciliation done"
                payload = report.asLogPayload()
            }
        }
    }

    @Suppress("TooGenericExceptionCaught") // 청크 단위 격리: MGET 실패가 전체 배치를 막지 않도록
    private fun applyChunk(
        chunk: List<ZoneExpected>,
        acc: DriftReport.Accumulator,
    ) {
        val actuals =
            try {
                stockRedisGateway.getAll(chunk.map { it.zoneId }) // 재고 조회
            } catch (e: Exception) {
                acc.failed += chunk.size
                logger.atError {
                    message = "Drift chunk MGET failed (left as drift, retried next run)"
                    cause = e
                    payload = mapOf("zoneIds" to chunk.map { it.zoneId })
                }
                return
            }

        chunk.forEach { plan -> applyDriftSafely(plan, actuals[plan.zoneId], acc) }
    }

    @Suppress("TooGenericExceptionCaught") // zone 단위 격리: 한 zone 실패가 배치를 막지 않도록
    private fun applyDriftSafely(
        plan: ZoneExpected,
        actual: Int?,
        acc: DriftReport.Accumulator,
    ) {
        try {
            when {
                // 키 부재 → skip (rebuild 책임)
                actual == null -> acc.skipped++
                // 누수 → 보정
                actual < plan.expected -> {
                    fixLeak(plan, actual)
                    acc.fixed++
                }
                // 오버셀 → 알림만
                actual > plan.expected -> {
                    alertOversell(plan, actual)
                    acc.oversell++
                }
                // 동일 → 무동작
                else -> acc.unchanged++
            }
        } catch (e: Exception) {
            // runCatching 미사용: Error 는 전파
            acc.failed++
            logger.atError {
                message = "Drift failed for one zone (left as drift, retried next run)"
                cause = e
                payload = mapOf("zoneId" to plan.zoneId)
            }
        }
    }

    private fun fixLeak(
        plan: ZoneExpected,
        actual: Int,
    ) {
        // stock 만 절대 SET(claimed 미접촉) — claimed 정합은 Rebuild 책임
        stockRedisGateway.correctStock(plan.zoneId, plan.expected)
        writeAudit(plan, actual) // best-effort (보정은 이미 적용됨)
        logger.atInfo {
            message = "Stock drift fixed (leak)"
            payload = mapOf("zoneId" to plan.zoneId, "actual" to actual, "expected" to plan.expected)
        }
    }

    @Suppress("TooGenericExceptionCaught") // 감사 best-effort: 실패해도 보정 결과(fixed)는 유지
    private fun writeAudit(
        plan: ZoneExpected,
        actual: Int,
    ) {
        try {
            auditLogRepository.insert(
                actorId = null,
                actionType = RedisAction.STOCK_DRIFT_FIX.name,
                targetType = "ZONE",
                targetId = plan.zoneId,
                details =
                    buildJsonObject {
                        put("actual", actual)
                        put("expected", plan.expected)
                    },
            )
        } catch (e: Exception) {
            logger.atWarn {
                message = "Drift reconciliation audit log failed (best-effort)"
                cause = e
                payload = mapOf("zoneId" to plan.zoneId)
            }
        }
    }

    private fun alertOversell(
        plan: ZoneExpected,
        actual: Int,
    ) {
        alertService.notify(
            AlertContext(
                trigger = AlertTrigger.STOCK_DRIFT_OVERSELL,
                zoneId = plan.zoneId.toString(),
                fields = mapOf("actual" to actual, "expected" to plan.expected),
            ),
        )
        logger.atWarn {
            message = "Stock drift oversell detected (alert only)"
            payload = mapOf("zoneId" to plan.zoneId, "actual" to actual, "expected" to plan.expected)
        }
    }
}
