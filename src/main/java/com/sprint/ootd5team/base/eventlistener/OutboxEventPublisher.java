package com.sprint.ootd5team.base.eventlistener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import com.sprint.ootd5team.domain.feed.entity.FeedEventOutbox;
import com.sprint.ootd5team.domain.feed.event.type.FeedContentUpdatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedDeletedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.repository.feedOutbox.FeedEventOutboxRepository;
import java.util.concurrent.TimeUnit;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox에 저장된 이벤트를 Kafka로 발행하는 Publisher
 *
 * <p>트랜잭션 커밋 후 OutboxCreatedEvent를 수신하여 즉시 Kafka 발행을 시도한다.
 * 실패 시 PENDING 상태로 남아 있다가 스케줄러가 재시도한다.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final FeedEventOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    /**
     * Outbox 이벤트를 Kafka로 발행
     *
     * @param outboxId Outbox 테이블의 PK
     */
    @Transactional
    public void publishOutboxEvent(Long outboxId) {
        FeedEventOutbox outbox = outboxRepository.findById(outboxId)
            .orElseThrow(() -> new IllegalStateException("Outbox not found: " + outboxId));

        if (outbox.getStatus() == EventStatus.PUBLISHED) {
            log.debug("[OutboxEventPublisher] 이미 발행됨 - outboxId: {}", outboxId);
            return;
        }

        if (outbox.getStatus() == EventStatus.CANCELLED) {
            log.debug("[OutboxEventPublisher] 취소된 이벤트 - outboxId: {}", outboxId);
            return;
        }

        try {
            Object eventPayload = deserializeEvent(outbox);

            String key = outbox.getAggregateId().toString();
            kafkaTemplate.send(outbox.getTopic(), key, eventPayload)
                .get(5, TimeUnit.SECONDS);

            outbox.markAsPublished();

            log.info("[OutboxEventPublisher] Kafka 발행 성공 - outboxId: {}, eventType: {}",
                outboxId, outbox.getEventType());

        } catch (Exception e) {
            outbox.incrementRetryCount();

            if (outbox.getRetryCount() > 5) {
                outbox.markAsFailed(e.getMessage());
                log.error("[OutboxEventPublisher] 재시도 한계 초과 - outboxId: {}, retryCount: {}",
                    outboxId, outbox.getRetryCount());
            } else {
                log.warn("[OutboxEventPublisher] Kafka 발행 실패 (재시도 {}/5) - outboxId: {}",
                    outbox.getRetryCount(), outboxId, e);
            }
        }
    }

    /**
     * Outbox payload(JSON)를 실제 이벤트 객체로 역직렬화
     */
    private Object deserializeEvent(FeedEventOutbox outbox) throws Exception {
        String payload = outbox.getPayload();
        String eventType = outbox.getEventType();

        return switch (eventType) {
            case "FeedIndexCreatedEvent" ->
                objectMapper.readValue(payload, FeedIndexCreatedEvent.class);
            case "FeedContentUpdatedEvent" ->
                objectMapper.readValue(payload, FeedContentUpdatedEvent.class);
            case "FeedLikeCountUpdateEvent" ->
                objectMapper.readValue(payload, FeedLikeCountUpdateEvent.class);
            case "FeedDeletedEvent" ->
                objectMapper.readValue(payload, FeedDeletedEvent.class);
            default ->
                throw new IllegalArgumentException("Unknown event type: " + eventType);
        };
    }
}