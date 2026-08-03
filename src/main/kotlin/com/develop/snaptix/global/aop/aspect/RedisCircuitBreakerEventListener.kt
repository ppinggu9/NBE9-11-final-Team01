package com.develop.snaptix.global.aop.aspect

import com.develop.snaptix.global.alert.model.AlertContext
import com.develop.snaptix.global.alert.model.AlertTrigger
import com.develop.snaptix.global.alert.service.AlertService
import com.develop.snaptix.global.redis.gateway.CanaryRedisGateway
import com.develop.snaptix.global.resilience.RebuildService
import io.github.oshai.kotlinlogging.KotlinLogging
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State.CLOSED
import io.github.resilience4j.circuitbreaker.CircuitBreaker.State.OPEN
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import jakarta.annotation.PostConstruct
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.util.concurrent.Executor

/**
 * Redis 서킷 상태 전이 리스너. (작업 명세서 v2.1 §9 · Story 13.1 → 13.2)
 *
 *  - OPEN  : 신규 주문 차단 알림(`CIRCUIT_OPEN`) — 기존 동작 유지.
 *  - CLOSED: 복구 신호 → 🆕 전용 executor 로 `RebuildService.rebuild()` 호출(리스너 스레드 블로킹 회피).
 *
 * `automatic-transition-from-open-to-half-open-enabled: true` 로 OPEN→HALF_OPEN→CLOSED 자동 복귀하며,
 * 중복 트리거는 executor `DiscardPolicy` + 코디네이터 락이 흡수(실제 재구축 1회).
 */

@Component
class RedisCircuitBreakerEventListener(
    private val circuitBreakerRegistry: CircuitBreakerRegistry,
    private val alertService: AlertService,
    private val rebuildService: RebuildService,
    private val canaryGateway: CanaryRedisGateway,
    @Qualifier("rebuildExecutor") private val rebuildExecutor: Executor,
) {
    private val logger = KotlinLogging.logger {}

    @PostConstruct
    fun registerListeners() {
        val circuitBreaker = circuitBreakerRegistry.circuitBreaker("redis")

        circuitBreaker
            .eventPublisher
            .onStateTransition { event ->
                val from = event.stateTransition.fromState.name
                val to = event.stateTransition.toState.name

                logger.atWarn {
                    message = "Redis circuit breaker state changed"
                    payload =
                        mapOf(
                            "action" to "CB_STATE_CHANGE",
                            "from" to from,
                            "to" to to,
                        )
                }

                when (event.stateTransition.toState) {
                    // 기존 유지: OPEN → 신규 주문 503 차단 알림
                    OPEN ->
                        alertService.notify(
                            AlertContext(
                                trigger = AlertTrigger.CIRCUIT_OPEN,
                                fields =
                                    mapOf(
                                        "circuitName" to "redis",
                                        "failureRate" to circuitBreaker.metrics.failureRate,
                                        "from" to from,
                                        "to" to to,
                                    ),
                            ),
                        )
                    // 🆕 CLOSED(복구) → 전용 executor 로 재구축 위임(블로킹 회피)
                    CLOSED ->
                        rebuildExecutor.execute {
                            if (canaryGateway.isAlive()) {
                                // 데이터 살아있음 = 무손실 트립 → rebuild 스킵 (플래핑·pause 억제)
                                logger.atInfo {
                                    message = "Rebuild skipped by canary: Redis data intact (transient trip)"
                                    payload = mapOf("action" to "REBUILD_CANARY_SKIP", "circuit" to "redis")
                                }
                            } else {
                                rebuildService.rebuild() // 데이터 소실 확정 → 재구축
                            }
                        }
                    else -> Unit
                }
            }
    }
}
