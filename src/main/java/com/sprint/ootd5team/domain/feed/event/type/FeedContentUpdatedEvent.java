package com.sprint.ootd5team.domain.feed.event.type;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
@JsonTypeName("feed-updated")
public class FeedContentUpdatedEvent extends FeedEvent {

    private final String content;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final long likeCount;

    public FeedContentUpdatedEvent(UUID feedId, String content, Instant createdAt, Instant updatedAt, long likeCount) {
        super(feedId);
        this.content = content;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.likeCount = likeCount;
    }
}