package com.develop.snaptix.global.redis.gateway

import com.develop.snaptix.global.redis.key.RedisKeyFactory
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.stereotype.Component

/**
 * 데이터 생존 카나리. 서킷 CLOSED 시 "실제 데이터 소실 여부"를 판별해 spurious rebuild를 막는다.
 *
 * ResilientRedisExecutor를 쓰지 않는다 — 이 체크의 실패를 서킷에 되먹이면 안 되고(재트립 유발),
 * 확인 불가 시엔 안전하게 "소실로 간주(rebuild 실행)"해야 하기 때문. 그래서 raw + runCatching.
 */
@Component
class CanaryRedisGateway(
    private val redis: StringRedisTemplate,
    private val keys: RedisKeyFactory,
) {
    /** 데이터가 정상 존재함을 표시 (이벤트 초기화·rebuild 성공 직후 호출). */
    fun markAlive() {
        runCatching { redis.opsForValue().set(keys.canary(), "1") }
    }

    /** true=데이터 생존(무손실 트립→skip) / false=소실 또는 확인 불가(→rebuild). */
    fun isAlive(): Boolean = runCatching {
        redis.hasKey(keys.canary()) == true
    }.getOrDefault(false)
}
