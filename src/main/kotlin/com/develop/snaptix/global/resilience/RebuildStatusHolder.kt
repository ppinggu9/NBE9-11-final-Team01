package com.develop.snaptix.global.resilience

import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicReference

/**
 * rebuild 마지막 실행 결과 홀더. (부하/카오스 테스트 관측용)
 *
 * rebuild는 서킷 CLOSED 시 전용 executor로 fire-and-forget 실행되어 동기 반환이 없다.
 * 그래서 마지막 결과(성공/실패/스킵·소요시간·시각)를 여기에 기록해두고,
 * [com.develop.snaptix.domain.reservation.controller.AdminRebuildController] 가 조회한다.
 *
 * 단일 값만 보관하므로 [AtomicReference] 로 스레드 안전하게 교체한다.
 * (관측 스냅샷 용도라 이력은 남기지 않음 — 필요 시 리스트/링버퍼로 확장.)
 */
@Component
class RebuildStatusHolder {
    private val last = AtomicReference<RebuildStatus?>(null)

    fun record(status: RebuildStatus) {
        last.set(status)
    }

    fun last(): RebuildStatus? = last.get()
}

/**
 * @property outcome    COMPLETED / FAILED / SKIPPED
 * @property events     재구축한 이벤트 수 (COMPLETED만)
 * @property zones      재구축한 zone 수 (COMPLETED만)
 * @property durationMs 실행 소요(ms)
 * @property error      실패 사유 (FAILED만)
 * @property startedAt  진입 시각 ISO-8601 (SKIPPED는 null)
 * @property finishedAt 기록 시각 ISO-8601
 */
data class RebuildStatus(
    val outcome: String,
    val events: Int? = null,
    val zones: Int? = null,
    val durationMs: Long? = null,
    val error: String? = null,
    val startedAt: String? = null,
    val finishedAt: String,
)
