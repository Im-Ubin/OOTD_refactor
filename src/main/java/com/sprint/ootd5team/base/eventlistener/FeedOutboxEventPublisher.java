package com.sprint.ootd5team.base.eventlistener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sprint.ootd5team.base.exception.feed.FeedOutboxNotFoundException;
import com.sprint.ootd5team.base.exception.feed.FeedUnknowEventTypeException;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Outbox 테이블에 저장된 이벤트를 Kafka로 발행하는 Publisher
 *
 * <p>트랜잭션 커밋 후 {@link FeedOutboxEventListener}로부터 outboxId를 전달받아
 * 즉시 Kafka 발행을 시도한다.</p>
 *
 * <p>발행 실패 시, retryCount를 증가시키고 PENDING 상태 유지
 * retryCount가 5를 초과하면 FAILED 상태로 전환하고 관리자에게 이메일 알림 발송</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FeedOutboxEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final FeedEventOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final JavaMailSender mailSender;

    @Value("${ootd.email.sender}")
    private String sender;

    @Value("${ootd.admin.email}")
    private String adminEmail;

    /**
     * Outbox 이벤트를 Kafka로 발행
     *
     * PENDING 또는 FAILED 상태인 경우에만 발행 시도
     *
     * @param outboxId 발행할 Outbox 레코드의 PK
     * @throws FeedOutboxNotFoundException outboxId에 해당하는 레코드가 없을 경우
     */
    @Transactional
    public void publishOutboxEvent(Long outboxId) {
        FeedEventOutbox outbox = outboxRepository.findById(outboxId)
            .orElseThrow(() -> FeedOutboxNotFoundException.withId(outboxId));

        if (outbox.getStatus() != EventStatus.PENDING
            && outbox.getStatus() != EventStatus.FAILED) {
            log.debug("[FeedOutboxEventPublisher] 발행 스킵 - outboxId: {}, status: {}",
                outboxId, outbox.getStatus());
            return;
        }

        try {
            Object eventPayload = deserializeEvent(outbox);
            String key = outbox.getAggregateId().toString();

            kafkaTemplate.send(outbox.getTopic(), key, eventPayload)
                .get(5, TimeUnit.SECONDS);

            outbox.markAsPublished();

            log.info("[FeedOutboxEventPublisher] Kafka 발행 성공 - outboxId: {}, eventType: {}",
                outboxId, outbox.getEventType());

        } catch (Exception e) {
            handlePublishFailure(outbox, outboxId, e);
        }
    }

    /**
     * Outbox payload(JSON)를 실제 이벤트 객체로 역직렬화
     *
     * @param outbox 역직렬화할 Outbox 엔티티
     * @return 역직렬화된 이벤트 객체
     * @throws Exception JSON 역직렬화 실패 시
     * @throws FeedUnknowEventTypeException 등록되지 않은 eventType인 경우
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
                throw FeedUnknowEventTypeException.withId(eventType);
        };
    }

    /**
     * Kafka 발행 실패 처리
     *
     * <p>retryCount를 증가시키고, 5회 초과 시 FAILED 상태로 전환 후
     * 관리자에게 이메일 알림 발송</p>
     *
     * @param outbox    발행 실패한 Outbox 엔티티
     * @param outboxId  로깅용 Outbox PK
     * @param e         발생한 예외
     */
    private void handlePublishFailure(FeedEventOutbox outbox, Long outboxId, Exception e) {
        outbox.incrementRetryCount();

        if (outbox.getRetryCount() > 5) {
            outbox.markAsFailed(e.getMessage());
            log.error("[FeedOutboxEventPublisher] 재시도 한계 초과 - outboxId: {}, retryCount: {}",
                outboxId, outbox.getRetryCount());

            try {
                sendFailureAlert(outbox);
            } catch (Exception mailException) {
                log.error("[FeedOutboxEventPublisher] 알림 이메일 발송 실패 - outboxId: {}", outboxId, mailException);
            }
        } else {
            log.warn("[FeedOutboxEventPublisher] Kafka 발행 실패 (재시도 {}/5) - outboxId: {}",
                outbox.getRetryCount(), outboxId, e);
        }
    }

    /**
     * FAILED 이벤트 발생 시 관리자에게 이메일 알림 발송
     *
     * @param outbox FAILED 상태로 전환된 Outbox 엔티티
     */
    private void sendFailureAlert(FeedEventOutbox outbox) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(sender);
        message.setTo(adminEmail);
        message.setSubject("[FeedOutboxEventPublisher] FeedOutbox 이벤트 발행 실패");
        message.setText(
            "Outbox 이벤트 발행이 최대 재시도 횟수를 초과했습니다.\n\n"
                + "outboxId: " + outbox.getId() + "\n"
                + "eventType: " + outbox.getEventType() + "\n"
                + "aggregateId: " + outbox.getAggregateId() + "\n"
                + "errorMessage: " + outbox.getErrorMessage()
        );
        mailSender.send(message);
    }
}