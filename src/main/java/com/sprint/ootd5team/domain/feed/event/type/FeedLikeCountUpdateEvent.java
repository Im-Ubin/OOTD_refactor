package com.sprint.ootd5team.domain.feed.event.type;

import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
public class FeedLikeCountUpdateEvent extends FeedEvent {

    private final long newLikeCount;
    private final Instant createdAt;
    private final Instant updatedAt;
    private final String content;

    public FeedLikeCountUpdateEvent(UUID feedId, long newLikeCount, Instant createdAt, Instant updatedAt, String content) {
        super(feedId);
        this.newLikeCount = newLikeCount;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.content = content;
    }
}
