package com.sprint.ootd5team.domain.feed.event;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import com.sprint.ootd5team.domain.feed.entity.FeedEventOutbox;
import com.sprint.ootd5team.domain.feed.event.publisher.FeedOutboxEventPublisher;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.repository.feedOutbox.FeedEventOutboxRepository;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@DisplayName("FeedOutboxEventPublisher 슬라이스 테스트")
public class FeedOutboxEventPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private FeedEventOutboxRepository outboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private JavaMailSender mailSender;

    @InjectMocks
    private FeedOutboxEventPublisher publisher;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(publisher, "sender", "sender@test.com");
        ReflectionTestUtils.setField(publisher, "adminEmail", "admin@test.com");
    }

    private FeedEventOutbox buildOutbox(EventStatus status, int retryCount) {
        return FeedEventOutbox.builder()
            .aggregateId(UUID.randomUUID())
            .eventType("FeedIndexCreatedEvent")
            .topic("ootd.Feeds.Created")
            .payload("{\"feedId\":\"" + UUID.randomUUID() + "\",\"content\":\"내용\",\"createdAt\":\"" + Instant.now() + "\"}")
            .status(status)
            .retryCount(retryCount)
            .build();
    }

    @Test
    @DisplayName("PENDING 상태 이벤트 Kafka 발행 성공 시 PUBLISHED로 변경")
    void publishOutboxEvent_pending_success() throws Exception {
        // given
        Long outboxId = 1L;
        FeedEventOutbox outbox = buildOutbox(EventStatus.PENDING, 0);

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));
        when(objectMapper.readValue(anyString(), eq(FeedIndexCreatedEvent.class)))
            .thenReturn(new FeedIndexCreatedEvent(UUID.randomUUID(), "내용", Instant.now()));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
            .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));

        // when
        publisher.publishOutboxEvent(outboxId);

        // then
        assertThat(outbox.getStatus()).isEqualTo(EventStatus.PUBLISHED);
    }

    @Test
    @DisplayName("PUBLISHED 상태 이벤트는 스킵")
    void publishOutboxEvent_published_skip() {
        // given
        Long outboxId = 1L;
        FeedEventOutbox outbox = buildOutbox(EventStatus.PUBLISHED, 0);

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));

        // when
        publisher.publishOutboxEvent(outboxId);

        // then
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("CANCELLED 상태 이벤트는 스킵")
    void publishOutboxEvent_cancelled_skip() {
        // given
        Long outboxId = 1L;
        FeedEventOutbox outbox = buildOutbox(EventStatus.CANCELLED, 0);

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));

        // when
        publisher.publishOutboxEvent(outboxId);

        // then
        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }

    @Test
    @DisplayName("Kafka 발행 실패 시 retryCount 증가")
    void publishOutboxEvent_kafkaFailure_incrementRetryCount() throws Exception {
        // given
        Long outboxId = 1L;
        FeedEventOutbox outbox = buildOutbox(EventStatus.PENDING, 0);

        when(outboxRepository.findById(outboxId)).thenReturn(Optional.of(outbox));
        when(objectMapper.readValue(anyString(), eq(FeedIndexCreatedEvent.class)))
            .thenReturn(new FeedIndexCreatedEvent(UUID.randomUUID(), "내용", Instant.now()));

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka error"));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(failedFuture);

        // when
        publisher.publishOutboxEvent(outboxId);

        // then
        assertThat(outbox.getRetryCount()).isEqualTo(1);
        assertThat(outbox.getStatus()).isEqualTo(EventStatus.PENDING);
    }

    @Test
    @DisplayName("retryCount 5 초과 시 FAILED 상태로 전환 및 이메일 알림")
    void publishOutboxEvent_retryExceeded_markAsFailed() throws Exception {
        // given
        Long outboxId = 1L;
        FeedEventOutbox outbox = buildOutbox(EventStatus.PENDING, 5);

        when(outboxRepository.findById(outboxId))
            .thenReturn(Optional.of(outbox));
        when(objectMapper.readValue(anyString(), eq(FeedIndexCreatedEvent.class)))
            .thenReturn(new FeedIndexCreatedEvent(UUID.randomUUID(), "내용", Instant.now()));

        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka error"));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
            .thenReturn(failedFuture);

        // when
        publisher.publishOutboxEvent(outboxId);

        // then
        assertThat(outbox.getStatus()).isEqualTo(EventStatus.FAILED);
        verify(mailSender).send(any(SimpleMailMessage.class));
    }
}