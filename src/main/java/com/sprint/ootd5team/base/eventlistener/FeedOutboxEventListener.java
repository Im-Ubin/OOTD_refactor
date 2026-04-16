package com.sprint.ootd5team.base.eventlistener;

import com.sprint.ootd5team.domain.feed.event.type.FeedOutboxSavedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedOutboxEventListener {

    private final FeedOutboxEventPublisher publisher;

    /**
     * 트랜잭션 커밋 후 OutboxCreatedEvent 수신
     */
    @Async("eventTaskExecutor")
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handleOutboxSaved(FeedOutboxSavedEvent event) {
        log.info("[OutboxEventListener] OutboxSavedEvent 수신 - outboxId: {}",
            event.getOutboxId());

        publisher.publishOutboxEvent(event.getOutboxId());
    }
}