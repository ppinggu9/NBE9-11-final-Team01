package com.develop.snaptix.domain.reservation.controller

import com.develop.snaptix.domain.reservation.service.DriftReconciliationService
import com.develop.snaptix.domain.reservation.service.DriftReport
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Profile
import org.springframework.http.ResponseEntity
import org.springframework.security.access.prepost.PreAuthorize
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.time.Clock
import java.time.Instant

@RestController
@RequestMapping("/api/v1/admin/drift")
@Profile("loadtest")
class AdminDriftController(
    private val driftReconciliationService: DriftReconciliationService,
    @Qualifier("alertClock") private val clock: Clock,
) {
    data class DriftTriggerResponse(
        val report: DriftReport,
        val durationMs: Long,
    )

    @PostMapping
    @PreAuthorize("hasRole('ADMIN')")
    fun drift(): ResponseEntity<DriftTriggerResponse> {
        val start = System.nanoTime()
        val report = driftReconciliationService.checkDrift(Instant.now(clock))
        val durationMs = (System.nanoTime() - start) / 1_000_000
        return ResponseEntity.ok(DriftTriggerResponse(report, durationMs))
    }
}
