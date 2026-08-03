package com.develop.snaptix.domain.event.service

import com.develop.snaptix.domain.event.dto.EventBulkCreateRequest
import com.develop.snaptix.domain.event.repository.EventInsertResult
import com.develop.snaptix.domain.order.config.OrderStreamProperties
import com.develop.snaptix.domain.zone.repository.ZoneInsertResult
import com.develop.snaptix.global.redis.gateway.CanaryRedisGateway
import com.develop.snaptix.global.redis.gateway.EventLifeCycleRedisGateway
import com.develop.snaptix.global.redis.gateway.schema.EventInfo
import org.springframework.stereotype.Component
import tools.jackson.databind.ObjectMapper
import java.time.Duration

private val EVENT_INFO_CACHE_TTL: Duration = Duration.ofHours(1)

@Component
class EventRedisInitializer(
    private val eventLifeCycleRedisGateway: EventLifeCycleRedisGateway,
    private val objectMapper: ObjectMapper,
    private val orderStreamProperties: OrderStreamProperties,
    private val canaryGateway: CanaryRedisGateway,
) {
    fun initialize(
        event: EventInsertResult,
        request: EventBulkCreateRequest,
        zones: List<ZoneInsertResult>,
    ) {
        val keys = buildKeys(event, zones)
        val arguments = buildArguments(event, request, zones)

        eventLifeCycleRedisGateway.initializeEventInfrastructure(keys, arguments)
        canaryGateway.markAlive()
    }

    private fun stockKey(zoneId: Long): String = "ZONE:$zoneId:stock"

    private fun eventInfoKey(eventId: String): String = "event:info:$eventId"

    private fun orderStreamKey(eventId: String): String = "queue:order:$eventId"

    private fun buildKeys(
        event: EventInsertResult,
        zones: List<ZoneInsertResult>,
    ): List<String> = buildList {
        add(eventInfoKey(event.publicId))
        add(orderStreamKey(event.publicId))
        zones.forEach { add(stockKey(it.id)) }
    }

    private fun buildArguments(
        event: EventInsertResult,
        request: EventBulkCreateRequest,
        zones: List<ZoneInsertResult>,
    ): List<String> {
        val cacheJson = objectMapper.writeValueAsString(event.toEventInfo(request, zones))

        return buildList {
            add(EVENT_INFO_CACHE_TTL.seconds.toString())
            add(orderStreamProperties.consumerGroup)
            add(cacheJson)
            add(zones.size.toString())
            zones.forEach { add(it.totalCapacity.toString()) }
        }
    }

    private fun EventInsertResult.toEventInfo(
        request: EventBulkCreateRequest,
        zones: List<ZoneInsertResult>,
    ): EventInfo = EventInfo(
        eventId = publicId,
        name = request.name,
        description = request.description.orEmpty(),
        location = request.location,
        startTime = request.startTime.toInstant().toString(),
        endTime = request.endTime.toInstant().toString(),
        status = request.initialStatus.name,
        posterUrl = request.posterUrl.orEmpty(),
        totalCapacity = zones.sumOf { it.totalCapacity },
    )
}
