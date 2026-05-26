package com.sprint.ootd5team.domain.feed.entity;

import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@EntityListeners(AuditingEntityListener.class)
@Table(
    name = "tbl_feed_event_outbox",
    indexes = {
        @Index(name = "idx_outbox_status_created_at", columnList = "status, created_at"),
        @Index(name = "idx_outbox_published_at", columnList = "published_at")
    }
)
public class FeedEventOutbox {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private UUID aggregateId;

    @Column(nullable = false)
    private String eventType;

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;

    @CreatedDate
    @Column(columnDefinition = "timestamp with time zone", nullable = false, updatable = false)
    private Instant createdAt;

    @Column
    private Instant publishedAt;

    @Builder.Default
    @Column
    private Integer retryCount = 0;

    @Column
    private String errorMessage;

    public void markAsPublished() {
        this.status = EventStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public void markAsFailed(String errorMessage) {
        this.status = EventStatus.FAILED;
        this.errorMessage = errorMessage;
    }
}