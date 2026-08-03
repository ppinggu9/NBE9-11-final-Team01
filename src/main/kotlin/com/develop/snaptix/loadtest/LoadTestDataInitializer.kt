package com.develop.snaptix.loadtest

import com.develop.snaptix.domain.event.dto.EventBulkCreateRequest
import com.develop.snaptix.domain.event.dto.EventStatusUpdateRequest
import com.develop.snaptix.domain.event.dto.ZoneCreateRequest
import com.develop.snaptix.domain.event.entity.EventStatus
import com.develop.snaptix.domain.event.service.EventService
import com.develop.snaptix.domain.user.entity.UserRole
import com.develop.snaptix.domain.user.entity.UsersTable
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jetbrains.exposed.v1.core.eq
import org.jetbrains.exposed.v1.core.like
import org.jetbrains.exposed.v1.jdbc.batchInsert
import org.jetbrains.exposed.v1.jdbc.insert
import org.jetbrains.exposed.v1.jdbc.selectAll
import org.jetbrains.exposed.v1.jdbc.transactions.transaction
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.context.annotation.Profile
import org.springframework.security.crypto.password.PasswordEncoder
import org.springframework.stereotype.Component
import java.io.File
import java.time.OffsetDateTime
import java.time.ZoneOffset

private val logger = KotlinLogging.logger {}
private const val SEP = "════════════════════════════════════════════════"

/**
 * loadtest 프로파일 활성 시 앱 기동과 함께 아래를 자동 수행한다. (멀티 zone 버전)
 *
 *  1. 어드민 계정 생성 (멱등)
 *  2. 테스트 유저 200명 생성 (멱등)
 *  3. 이벤트 1개 + **구역(zone) N개**(A/B/C/D) 생성 → ON_SALE (매 기동마다 새로 생성)
 *  4. loadtest/seed/.env 에 EVENT_ID / ZONE_ID / REDIS_STOCK_KEY(첫 zone, 단일-스크립트 호환)
 *     + ZONE_DB_IDS / REDIS_STOCK_KEYS(전체 zone, 멀티-스크립트용) 기록
 *  5. loadtest/seed/users.json 에 유저 목록 기록
 *
 * 도메인: event=공연장/행사, zone=구역(A/B/C/D 등, 각 구역에 좌석 수).
 *        세부 좌석 위치는 프런트 미구현으로 생략.
 */
@Component
@Profile("loadtest")
class LoadTestDataInitializer(
    private val passwordEncoder: PasswordEncoder,
    private val eventService: EventService,
) : ApplicationRunner {
    companion object {
        private const val ADMIN_EMAIL = "admin@snaptix.kr"
        private const val ADMIN_PASSWORD = "Admin1234!"

        private const val USER_EMAIL_PREFIX = "load-user"
        private const val USER_EMAIL_DOMAIN = "test.com"
        private const val USER_PASSWORD = "Test1234!"
        private const val USER_COUNT = 2000

        private const val EVENT_NAME = "Load Test Event"
        private const val UNIT_PRICE = 10_000

        // ★ 멀티 zone: 구역 이름과 구역당 좌석 수. 구역 수를 늘리려면 여기만 수정.
        private val ZONE_NAMES = listOf("A구역", "B구역", "C구역", "D구역")
        private const val PER_ZONE_CAPACITY = 500 // 구역당 좌석 수 (4구역 × 100 = 총 400석)

        private const val SEED_ENV_PATH = "loadtest/seed/.env"
        private const val USERS_JSON_PATH = "loadtest/seed/users.json"
    }

    override fun run(args: ApplicationArguments) {
        logger.info { "[LOADTEST] 시드 초기화 시작" }

        seedAdmin()
        seedUsers()
        val result = seedEvent()

        writeSeedEnv(result)
        writeUsersJson()
        printSummary(result)
    }

    // ── STEP 1: 어드민 ──────────────────────────────────────────────────────────

    private fun seedAdmin() {
        val exists =
            transaction {
                UsersTable
                    .selectAll()
                    .where { UsersTable.email eq ADMIN_EMAIL }
                    .count() > 0
            }
        if (exists) {
            logger.info { "[LOADTEST] 어드민 이미 존재: $ADMIN_EMAIL" }
            return
        }
        val encodedAdminPw =
            requireNotNull(passwordEncoder.encode(ADMIN_PASSWORD)) { "PasswordEncoder returned null" }
        transaction {
            UsersTable.insert {
                it[email] = ADMIN_EMAIL
                it[password] = encodedAdminPw
                it[role] = UserRole.ADMIN.name
            }
        }
        logger.info { "[LOADTEST] 어드민 created: $ADMIN_EMAIL" }
    }

    // ── STEP 2: 테스트 유저 ─────────────────────────────────────────────────────

    private fun seedUsers() {
        val existingCount =
            transaction {
                UsersTable
                    .selectAll()
                    .where { UsersTable.email like "$USER_EMAIL_PREFIX-%@$USER_EMAIL_DOMAIN" }
                    .count()
            }

        if (existingCount >= USER_COUNT) {
            logger.info { "[LOADTEST] 테스트 유저 이미 존재 ($existingCount 명) — 스킵" }
            return
        }

        val encodedPw =
            requireNotNull(passwordEncoder.encode(USER_PASSWORD)) { "PasswordEncoder returned null" }
        val startIdx = existingCount + 1

        transaction {
            UsersTable.batchInsert(data = (startIdx..USER_COUNT.toLong()).toList(), ignore = true) { idx ->
                this[UsersTable.email] = "$USER_EMAIL_PREFIX-$idx@$USER_EMAIL_DOMAIN"
                this[UsersTable.password] = encodedPw
                this[UsersTable.role] = UserRole.USER.name
            }
        }
        logger.info { "[LOADTEST] 테스트 유저 생성: $USER_COUNT 명" }
    }

    // ── STEP 3: 이벤트 + 멀티 zone 생성 + ON_SALE ───────────────────────────────

    private data class ZoneSeed(
        val zoneId: String, // public UUID
        val redisStockKey: String, // "ZONE:<internalId>:stock"
    )

    private data class SeedResult(
        val eventId: String,
        val zones: List<ZoneSeed>,
    )

    private fun seedEvent(): SeedResult {
        val now = OffsetDateTime.now(ZoneOffset.UTC)

        val createResponse =
            eventService.createEventWithZones(
                EventBulkCreateRequest(
                    name = EVENT_NAME,
                    description = "k6 부하 테스트용 이벤트 (멀티 zone)",
                    location = "SnapTix 테스트 홀",
                    startTime = now.plusYears(1),
                    endTime = now.plusYears(1).plusHours(3),
                    initialStatus = EventStatus.PENDING,
                    // ★ 구역 N개 생성
                    zones =
                        ZONE_NAMES.map { name ->
                            ZoneCreateRequest(
                                name = name,
                                unitPrice = UNIT_PRICE,
                                totalCapacity = PER_ZONE_CAPACITY,
                            )
                        },
                ),
            )

        eventService.updateEventStatus(
            createResponse.eventId,
            EventStatusUpdateRequest(EventStatus.ON_SALE),
        )

        val zones = createResponse.registeredZones.map { ZoneSeed(it.zoneId, it.redisStockKey) }

        logger.info { "[LOADTEST] 이벤트 생성 완료 → ON_SALE (zones=${zones.size})" }
        logger.info { "[LOADTEST] EVENT_ID = ${createResponse.eventId}" }
        zones.forEachIndexed { i, z ->
            logger.info { "[LOADTEST] ZONE[$i] id=${z.zoneId} stockKey=${z.redisStockKey}" }
        }

        return SeedResult(createResponse.eventId, zones)
    }

    // ── STEP 4: loadtest/seed/.env 저장 ─────────────────────────────────────────

    private fun writeSeedEnv(result: SeedResult) {
        val envFile = File(SEED_ENV_PATH).also { it.parentFile?.mkdirs() }
        val first = result.zones.first()
        // 내부 zoneId 목록(=REDIS_STOCK_KEY의 가운데 숫자) / 전체 stock 키 목록
        val zoneDbIds = result.zones.joinToString(",") { it.redisStockKey.split(":")[1] }
        val stockKeys = result.zones.joinToString(",") { it.redisStockKey }

        val preserved =
            if (envFile.exists()) {
                envFile.readLines().filterNot { line ->
                    line.startsWith("EVENT_ID=") ||
                        line.startsWith("ZONE_ID=") ||
                        line.startsWith("REDIS_STOCK_KEY=") ||
                        line.startsWith("ZONE_DB_IDS=") ||
                        line.startsWith("REDIS_STOCK_KEYS=")
                }
            } else {
                emptyList()
            }

        val lines =
            preserved +
                listOf(
                    "EVENT_ID=${result.eventId}",
                    // 단일-zone 스크립트(order-load/sse-reconnect) 호환: 첫 zone
                    "ZONE_ID=${first.zoneId}",
                    "REDIS_STOCK_KEY=${first.redisStockKey}",
                    // 멀티-zone 스크립트(redis-recovery)용: 전체
                    "ZONE_DB_IDS=$zoneDbIds",
                    "REDIS_STOCK_KEYS=$stockKeys",
                )
        envFile.writeText(lines.joinToString("\n", postfix = "\n"))
        logger.info { "[LOADTEST] $SEED_ENV_PATH 저장 완료 (zones=${result.zones.size})" }
    }

    // ── STEP 5: loadtest/seed/users.json 저장 ──────────────────────────────────

    private fun writeUsersJson() {
        val entries =
            (1..USER_COUNT).joinToString(",\n  ") { i ->
                """{"email":"$USER_EMAIL_PREFIX-$i@$USER_EMAIL_DOMAIN","password":"$USER_PASSWORD"}"""
            }
        File(USERS_JSON_PATH)
            .also { it.parentFile?.mkdirs() }
            .writeText("[\n  $entries\n]\n")
        logger.info { "[LOADTEST] $USERS_JSON_PATH 저장 완료 ($USER_COUNT 명)" }
    }

    // ── 완료 요약 ────────────────────────────────────────────────────────────────

    private fun printSummary(result: SeedResult) {
        logger.info { "[LOADTEST] $SEP" }
        logger.info { "[LOADTEST]  시드 완료 (멀티 zone) — seed.sh 없이 자동 생성됩니다" }
        logger.info { "[LOADTEST]  어드민: $ADMIN_EMAIL / $ADMIN_PASSWORD" }
        logger.info { "[LOADTEST]  유저  : $USER_EMAIL_PREFIX-1~$USER_COUNT@$USER_EMAIL_DOMAIN / $USER_PASSWORD" }
        logger.info { "[LOADTEST]  EVENT_ID = ${result.eventId}" }
        logger.info {
            "[LOADTEST]  ZONES    = ${result.zones.size} (${ZONE_NAMES.joinToString(
                ",",
            )}) × ${PER_ZONE_CAPACITY}석"
        }
        logger.info { "[LOADTEST] $SEP" }
    }
}
