package com.develop.snaptix.global.redis.gateway

import com.develop.snaptix.global.redis.key.RedisKeyFactory
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.data.redis.core.StringRedisTemplate
import org.springframework.data.redis.core.ValueOperations

/**
 * CanaryRedisGateway 단위 테스트 (MockK).
 *
 * 카나리 = "Redis에 유효한 데이터가 있다"는 생존 표식. 서킷 CLOSED 시 이 키 존재 여부로
 * spurious rebuild(무손실 트립)를 걸러낸다. 검증 포인트:
 *  - markAlive: TTL 없이 SET (만료되면 데이터가 있는데도 소실로 오판하므로)
 *  - isAlive  : 존재→true / 없음·null→false
 *  - 예외 안전측: 확인 불가/기록 실패 시에도 서킷에 되먹이지 않음(전파 X, 소실 간주)
 */
class CanaryRedisGatewayTest {
    private val redis = mockk<StringRedisTemplate>()
    private val valueOps = mockk<ValueOperations<String, String>>(relaxUnitFun = true)
    private val keys = mockk<RedisKeyFactory>()
    private val gateway = CanaryRedisGateway(redis, keys)

    private val canaryKey = "snaptix:redis:canary"

    @BeforeEach
    fun setUp() {
        every { keys.canary() } returns canaryKey
        every { redis.opsForValue() } returns valueOps
    }

    @Test
    fun `markAlive는 카나리 키를 TTL 없이 SET한다`() {
        gateway.markAlive()

        // TTL 인자 없는 set 오버로드 → 만료 없음(persist)
        verify(exactly = 1) { valueOps.set(canaryKey, "1") }
    }

    @Test
    fun `isAlive는 카나리 키가 존재하면 true를 반환한다`() {
        every { redis.hasKey(canaryKey) } returns true

        assertThat(gateway.isAlive()).isTrue()
    }

    @Test
    fun `isAlive는 카나리 키가 없으면 false를 반환한다`() {
        every { redis.hasKey(canaryKey) } returns false

        assertThat(gateway.isAlive()).isFalse()
    }

    @Test
    fun `isAlive는 hasKey가 null이어도 false를 반환한다`() {
        every { redis.hasKey(canaryKey) } returns null

        assertThat(gateway.isAlive()).isFalse() // '== true' null-safe 처리
    }

    @Test
    fun `isAlive는 Redis 예외 시 안전측으로 false를 반환한다`() {
        every { redis.hasKey(any()) } throws RuntimeException("redis down")

        assertThat(gateway.isAlive()).isFalse() // 확인 불가 → 소실 간주(rebuild 유도)
    }

    @Test
    fun `markAlive는 Redis 예외를 삼켜 전파하지 않는다`() {
        every { valueOps.set(any(), any()) } throws RuntimeException("redis down")

        gateway.markAlive() // 예외가 던져지지 않으면 통과 (서킷 되먹임 방지)
    }
}
