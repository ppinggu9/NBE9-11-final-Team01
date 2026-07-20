package com.develop.snaptix.domain.reservation.controller

import com.develop.snaptix.global.resilience.RebuildStatus
import com.develop.snaptix.global.resilience.RebuildStatusHolder
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

/**
 * rebuild 마지막 실행 상태 조회. (부하/카오스 테스트 관측용)
 *
 * [AdminReconcileController] 와 **동일 패턴**:
 * ADMIN 전용(`@PreAuthorize`), 도메인 서비스/홀더에 위임만 한다.
 * 차이는 정산 트리거(POST)가 아니라 **rebuild 결과 조회(GET)** 라는 점.
 *
 * rebuild는 서킷 CLOSED 시 fire-and-forget이라 동기 반환이 없으므로,
 * [com.develop.snaptix.global.resilience.RebuildStatusHolder]가 보관한 마지막 결과를 노출한다. Slack/15초 scrape에 의존하지 않고
 * k6가 이 엔드포인트를 폴링해 "언제 rebuild가 돌았고 성공/실패했는지"를 결정적으로 관측한다.
 *
 * 응답: 200 + [com.develop.snaptix.global.resilience.RebuildStatus] / 아직 한 번도 안 돌았으면 204 No Content.
 */
@RestController
@RequestMapping("/api/v1/admin/rebuild")
class AdminRebuildController(
    private val rebuildStatusHolder: RebuildStatusHolder,
) {
    @GetMapping("/last")
    @PreAuthorize("hasRole('ADMIN')")
    fun last(): ResponseEntity<RebuildStatus> = rebuildStatusHolder
        .last()
        ?.let { ResponseEntity.ok(it) }
        ?: ResponseEntity.noContent().build()
}
