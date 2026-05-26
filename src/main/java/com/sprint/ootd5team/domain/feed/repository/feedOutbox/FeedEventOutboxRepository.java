package com.sprint.ootd5team.domain.feed.repository.feedOutbox;

import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import com.sprint.ootd5team.domain.feed.entity.FeedEventOutbox;
import io.lettuce.core.dynamic.annotation.Param;
import java.time.Instant;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface FeedEventOutboxRepository extends JpaRepository<FeedEventOutbox, Long> {

    List<FeedEventOutbox> findByStatusAndCreatedAtBefore(
        EventStatus status,
        Instant createdAt,
        Pageable pageable
    );

    @Modifying
    @Query("DELETE FROM FeedEventOutbox o WHERE o.status = :status AND o.publishedAt < :before")
    void deleteByStatusAndPublishedAtBefore(
        @Param("status") EventStatus status,
        @Param("before") Instant before
    );

    long countByStatus(EventStatus status);
}