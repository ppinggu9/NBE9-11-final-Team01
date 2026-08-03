package com.develop.snaptix.global.resilience.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component
import java.time.Duration

/**
 * Reconcile / Rebuild / Drift 공통 설정. (작업 명세서 5·11-1)
 *
 * application.yaml `reconcile.*` 바인딩.
 *  - hold-window      : 유효 점유 홀드 윈도우 (기본 5분, `cutoff = now − holdWindow`)
 *  - scheduler-cron   : ReconcileScheduler 주기 (기본 10분)
 *  - drift-cron       : DriftReconciliationScheduler 주기 (기본 30분)
 *  - rebuild-lock-ttl : RebuildCoordinator 단일 실행 락 TTL (기본 5분)
 */
@Component
@ConfigurationProperties(prefix = "reconcile")
class ReconcileProperties {
    var holdWindow: Duration = Duration.ofMinutes(5)
    var schedulerCron: String = "0 */10 * * * *"
    var driftCron: String = "0 */30 * * * *"
    var rebuildLockTtl: Duration = Duration.ofMinutes(5)
    var driftChunkSize: Int = 500
}
