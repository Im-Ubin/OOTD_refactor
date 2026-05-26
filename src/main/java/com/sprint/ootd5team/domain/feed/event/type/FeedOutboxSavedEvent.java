package com.sprint.ootd5team.domain.feed.event.type;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Outbox에 이벤트가 저장되었음을 알리는 Spring 내부 이벤트
 * TransactionalEventListener가 이 이벤트를 받아서 Kafka 발행 시도
 */
@Getter
@RequiredArgsConstructor
public class FeedOutboxSavedEvent {
    private final Long outboxId;
}