package com.develop.snaptix.global.resilience

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

/**
 * RebuildStatusHolder 단위 테스트.
 * 관측 스냅샷 홀더의 계약만 검증한다.
 *  - 초기 상태: last() == null (아직 rebuild 없음 → 컨트롤러가 204 반환)
 *  - record 후: 그 값을 그대로 반환 (동일 인스턴스)
 *  - 여러 번 record: 최신 값만 유지 (이력 없음)
 */
class RebuildStatusHolderTest {
    private val holder = RebuildStatusHolder()

    @Test
    fun `초기에는 last가 null이다`() {
        assertNull(holder.last())
    }

    @Test
    fun `record한 값을 last로 그대로 반환한다`() {
        val status =
            RebuildStatus(
                outcome = "COMPLETED",
                events = 1,
                zones = 4,
                durationMs = 12,
                startedAt = "2026-06-25T03:00:00Z",
                finishedAt = "2026-06-25T03:00:00Z",
            )

        holder.record(status)

        assertSame(status, holder.last()) // 동일 인스턴스 반환
    }

    @Test
    fun `여러 번 record하면 마지막 값만 유지된다`() {
        val skipped = RebuildStatus(outcome = "SKIPPED", finishedAt = "2026-06-25T03:00:00Z")
        val completed =
            RebuildStatus(
                outcome = "COMPLETED",
                events = 1,
                zones = 4,
                durationMs = 5,
                finishedAt = "2026-06-25T03:01:00Z",
            )

        holder.record(skipped)
        holder.record(completed)

        assertSame(completed, holder.last())
        assertEquals("COMPLETED", holder.last()?.outcome)
    }
}
