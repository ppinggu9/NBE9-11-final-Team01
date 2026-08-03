package com.develop.snaptix.global.aop.aspect

import com.develop.snaptix.global.alert.model.AlertContext
import com.develop.snaptix.global.alert.model.AlertTrigger
import com.develop.snaptix.global.alert.service.AlertService
import com.develop.snaptix.global.redis.gateway.CanaryRedisGateway
import com.develop.snaptix.global.resilience.RebuildService
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.Executor

class RedisCircuitBreakerEventListenerTest {
    private val alertService = mockk<AlertService>(relaxUnitFun = true) // unit 자동반환
    private val rebuildService = mockk<RebuildService>(relaxUnitFun = true)
    private val canaryGateway = mockk<CanaryRedisGateway>()
    private val syncExecutor = Executor { it.run() } // 동기 실행(같은 스레드)

    private lateinit var registry: CircuitBreakerRegistry
    private lateinit var circuitBreaker: CircuitBreaker

    @BeforeEach
    fun setUp() {
        registry = CircuitBreakerRegistry.ofDefaults()
        circuitBreaker = registry.circuitBreaker("redis", CircuitBreakerConfig.custom().build())
        every { canaryGateway.isAlive() } returns false
    }

    /** 리스너 생성+등록. executor 만 테스트별로 달라질 수 있어 인자로 받는다(기본: 동기). */
    private fun registerListener(executor: Executor = syncExecutor) {
        RedisCircuitBreakerEventListener(
            circuitBreakerRegistry = registry,
            alertService = alertService,
            rebuildService = rebuildService,
            canaryGateway = canaryGateway,
            rebuildExecutor = executor,
        ).registerListeners()
    }

    /** 비-OPEN(FORCED_OPEN, 알림 없음) → CLOSED 복구 전이. */
    private fun forcedOpenToClosed() {
        circuitBreaker.transitionToForcedOpenState()
        circuitBreaker.transitionToClosedState()
    }

    @Test
    fun `Redis circuit breaker가 OPEN으로 전이되면 Slack 알림을 요청한다`() {
        registerListener()
        val alertContextSlot = slot<AlertContext>()

        circuitBreaker.transitionToOpenState()

        verify(exactly = 1) { alertService.notify(capture(alertContextSlot)) }
        assertThat(alertContextSlot.captured.trigger).isEqualTo(AlertTrigger.CIRCUIT_OPEN)
        assertThat(alertContextSlot.captured.fields["circuitName"]).isEqualTo("redis")
        assertThat(alertContextSlot.captured.fields).containsKey("failureRate")
        assertThat(alertContextSlot.captured.fields["from"]).isEqualTo("CLOSED")
        assertThat(alertContextSlot.captured.fields["to"]).isEqualTo("OPEN")
        verify(exactly = 0) { rebuildService.rebuild() } // OPEN 전이는 재구축 아님
    }

    @Test
    fun `Redis circuit breaker가 OPEN이 아닌 상태로 전이되면 Slack 알림을 요청하지 않는다`() {
        registerListener()

        forcedOpenToClosed()

        verify(exactly = 0) { alertService.notify(any()) }
    }

    @Test
    fun `HALF_OPEN에서 OPEN으로 다시 전이되면 Slack 알림을 다시 요청한다`() {
        registerListener()
        val alertContexts = mutableListOf<AlertContext>()

        circuitBreaker.transitionToOpenState()
        circuitBreaker.transitionToHalfOpenState()
        circuitBreaker.transitionToOpenState()

        verify(exactly = 2) { alertService.notify(capture(alertContexts)) }
        assertThat(alertContexts[0].fields["from"]).isEqualTo("CLOSED")
        assertThat(alertContexts[0].fields["to"]).isEqualTo("OPEN")
        assertThat(alertContexts[1].fields["from"]).isEqualTo("HALF_OPEN")
        assertThat(alertContexts[1].fields["to"]).isEqualTo("OPEN")
    }

    /**
     * 실제 자동 복구 경로(OPEN → HALF_OPEN → CLOSED)에서 rebuild 가 CLOSED 에서만 1회 트리거되는지.
     * (참고: `automaticTransitionFromOpenToHalfOpenEnabled`는 시간 기반 자동 전이 설정이라
     *  수동 transition 테스트에선 영향이 없어 생략 — 검증 대상은 상태 시퀀스 자체다.)
     */
    @Test
    fun `자동 복구 경로 OPEN_HALF_OPEN_CLOSED 에서 OPEN 알림 1회와 rebuild 1회가 발생한다`() {
        registerListener()

        circuitBreaker.transitionToOpenState() // OPEN → 알림
        circuitBreaker.transitionToHalfOpenState() // HALF_OPEN → 무동작
        circuitBreaker.transitionToClosedState() // CLOSED → rebuild

        verify(exactly = 1) { rebuildService.rebuild() } // CLOSED 에서만 1회
        verify(exactly = 1) { alertService.notify(any()) } // OPEN 에서만 1회
    }

    /**
     * CLOSED 전이 시 리스너가 rebuild 를 **직접 호출하지 않고** rebuildExecutor 에 위임하는지(블로킹 회피 의도).
     * mock executor 가 Runnable 을 실행하지 않으면 rebuild 는 아직 호출되지 않아야 한다.
     */
    @Test
    fun `CLOSED 전이 시 rebuild를 직접 호출하지 않고 rebuildExecutor에 위임한다`() {
        val mockExecutor = mockk<Executor>()
        val runnableSlot = slot<Runnable>()
        every { mockExecutor.execute(capture(runnableSlot)) } returns Unit
        registerListener(mockExecutor)

        forcedOpenToClosed()

        // 1) executor 로 위임 + 리스너 스레드에서 직접 rebuild 안 함(아직 미실행)
        verify(exactly = 1) { mockExecutor.execute(any()) }
        verify(exactly = 0) { rebuildService.rebuild() }
        // 2) 위임된 Runnable 이 rebuild 를 감싼다
        runnableSlot.captured.run()
        verify(exactly = 1) { rebuildService.rebuild() }
    }

    @Test
    fun `카나리 생존이면 CLOSED에서 rebuild를 스킵한다`() {
        every { canaryGateway.isAlive() } returns true // 데이터 살아있음(무손실 트립)
        registerListener()

        forcedOpenToClosed()

        verify(exactly = 1) { canaryGateway.isAlive() }
        verify(exactly = 0) { rebuildService.rebuild() } // ★ 스킵
    }

    @Test
    fun `카나리 소실이면 CLOSED에서 rebuild를 실행한다`() {
        every { canaryGateway.isAlive() } returns false // 데이터 소실 확정
        registerListener()

        forcedOpenToClosed()

        verify(exactly = 1) { canaryGateway.isAlive() }
        verify(exactly = 1) { rebuildService.rebuild() } // ★ 실행
    }
}
