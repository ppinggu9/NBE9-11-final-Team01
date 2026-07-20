package com.develop.snaptix.global.resilience

import com.develop.snaptix.domain.event.repository.EventDetail
import com.develop.snaptix.domain.reservation.repository.EventRebuildData
import com.develop.snaptix.domain.reservation.repository.RebuildSnapshot
import com.develop.snaptix.domain.reservation.repository.ZoneRebuildData
import com.develop.snaptix.domain.reservation.service.ReconcileReport
import com.develop.snaptix.domain.reservation.service.ReconcileService
import com.develop.snaptix.global.alert.model.AlertContext
import com.develop.snaptix.global.alert.model.AlertTrigger
import com.develop.snaptix.global.alert.service.AlertService
import com.develop.snaptix.global.observability.RebuildMetrics
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import io.mockk.verifyOrder
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset

/**
 * RebuildService 단위 테스트 (MockK). #7 부품을 모두 목으로 대체해 **조립 동작**만 검증한다.
 *  - 락 미획득 → no-op(어떤 부수효과도 없음)
 *  - 정상: enable → (a)reconcile → snapshot → (b)(c+d)write → COMPLETED → finally disable·release (순서)
 *  - 중간 예외 → REBUILD_FAILED + finally 에서 disable·release 보장
 */
class RebuildServiceUnitTest {
    private val reconcileService = mockk<ReconcileService>()
    private val snapshotReader = mockk<RebuildSnapshotReader>()
    private val rebuildMetrics = mockk<RebuildMetrics>(relaxUnitFun = true)
    private val writer = mockk<RebuildRedisWriter>(relaxUnitFun = true)
    private val coordinator = mockk<RebuildCoordinator>(relaxUnitFun = true)
    private val readOnly = mockk<ReadOnlyModeHolder>(relaxUnitFun = true)
    private val alertService = mockk<AlertService>(relaxUnitFun = true)
    private val rebuildStatusHolder = mockk<RebuildStatusHolder>(relaxUnitFun = true) // ★ 추가
    private val fixedInstant = Instant.parse("2026-06-25T03:00:00Z")
    private val clock = Clock.fixed(fixedInstant, ZoneOffset.UTC)

    private val service =
        RebuildService(
            reconcileService,
            snapshotReader,
            writer,
            coordinator,
            readOnly,
            rebuildMetrics,
            rebuildStatusHolder,
            alertService,
            clock,
        )

    private fun snapshotOf(): RebuildSnapshot {
        val event = mockk<EventDetail>()
        return RebuildSnapshot(
            listOf(
                EventRebuildData(
                    event = event,
                    totalCapacity = 100,
                    zones = listOf(ZoneRebuildData(zoneId = 1L, stock = 60, claimedOrderIds = listOf("o1", "o2"))),
                ),
            ),
        )
    }

    @Test
    fun `should_no_op_when_락_미획득이면`() {
        every { coordinator.tryAcquire() } returns false

        service.rebuild()

        verify(exactly = 0) { readOnly.enable() }
        verify(exactly = 0) { reconcileService.reconcileExpired(any()) }
        verify(exactly = 0) { snapshotReader.read(any()) }
        verify(exactly = 0) { writer.writeEventInfo(any(), any()) }
        verify(exactly = 0) { alertService.notify(any()) }
        verify(exactly = 1) { rebuildStatusHolder.record(match { it.outcome == "SKIPPED" }) }
        verify(exactly = 0) { coordinator.release() } // 락 못 잡았으면 해제도 없음(早期 return)
    }

    @Test
    fun `should_고정순서로_재구축하고_완료알림_when_락_획득이면`() {
        every { coordinator.tryAcquire() } returns true
        every { reconcileService.reconcileExpired(fixedInstant) } returns ReconcileReport(0, 0, 0)
        every { snapshotReader.read(fixedInstant) } returns snapshotOf()

        service.rebuild()

        // now 1회 고정 → reconcile·snapshot 모두 fixedInstant 사용 + 고정 순서
        verifyOrder {
            coordinator.tryAcquire()
            readOnly.enable()
            alertService.notify(match<AlertContext> { it.trigger == AlertTrigger.REBUILD_STARTED })
            reconcileService.reconcileExpired(fixedInstant)
            snapshotReader.read(fixedInstant)
            writer.writeEventInfo(any(), 100)
            writer.rebuildZone(1L, 60, listOf("o1", "o2"))
            alertService.notify(match<AlertContext> { it.trigger == AlertTrigger.REBUILD_COMPLETED })
            readOnly.disable()
            coordinator.release()
        }
        verify(exactly = 1) {
            rebuildStatusHolder.record(match { it.outcome == "COMPLETED" && it.events == 1 && it.zones == 1 })
        }
    }

    @Test
    fun `should_REBUILD_FAILED와_finally해제_when_중간에_예외가_나면`() {
        every { coordinator.tryAcquire() } returns true
        every { reconcileService.reconcileExpired(any()) } returns ReconcileReport(0, 0, 0)
        every { snapshotReader.read(any()) } throws RuntimeException("snapshot failed")

        service.rebuild()

        verify(exactly = 1) {
            alertService.notify(match<AlertContext> { it.trigger == AlertTrigger.REBUILD_FAILED })
        }
        verify(exactly = 0) {
            alertService.notify(match<AlertContext> { it.trigger == AlertTrigger.REBUILD_COMPLETED })
        }
        verify(exactly = 0) { writer.writeEventInfo(any(), any()) }

        verify(exactly = 1) {
            rebuildStatusHolder.record(match { it.outcome == "FAILED" && it.error == "snapshot failed" })
        }
        // 실패해도 finally 에서 반드시 해제
        verify(exactly = 1) { readOnly.disable() }
        verify(exactly = 1) { coordinator.release() }
    }
}
