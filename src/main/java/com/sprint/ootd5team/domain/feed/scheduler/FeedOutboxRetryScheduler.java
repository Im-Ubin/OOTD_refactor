package com.sprint.ootd5team.domain.feed.scheduler;

import com.sprint.ootd5team.base.eventlistener.FeedOutboxEventPublisher;
import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import com.sprint.ootd5team.domain.feed.entity.FeedEventOutbox;
import com.sprint.ootd5team.domain.feed.repository.feedOutbox.FeedEventOutboxRepository;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedOutboxRetryScheduler {

    private final FeedEventOutboxRepository outboxRepository;
    private final FeedOutboxEventPublisher publisher;

    @Scheduled(fixedDelay = 60000)
    public void retryPendingOutbox() {
        List<FeedEventOutbox> pendingList = outboxRepository.findByStatusAndCreatedAtBefore(
            EventStatus.PENDING,
            Instant.now().minusSeconds(60),
            PageRequest.of(0, 100)
        );

        log.info("[FeedOutboxRetryScheduler] PENDING 재시도 건수: {}", pendingList.size());

        for (FeedEventOutbox outbox : pendingList) {
            publisher.publishOutboxEvent(outbox.getId());
        }
    }

    @Scheduled(cron = "0 0 2 * * *")
    public void deletePublishedOutbox() {
        outboxRepository.deleteByStatusAndPublishedAtBefore(
            EventStatus.PUBLISHED,
            Instant.now().minus(7, ChronoUnit.DAYS)
        );

        log.info("[FeedOutboxRetryScheduler] PUBLISHED 이벤트 삭제 완료");
    }
}