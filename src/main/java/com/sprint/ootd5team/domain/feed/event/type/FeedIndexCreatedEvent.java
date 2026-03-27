package com.sprint.ootd5team.domain.feed.event.type;

import java.time.Instant;
import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
public class FeedIndexCreatedEvent extends FeedEvent {

    private final String content;
    private final Instant createdAt;

    public FeedIndexCreatedEvent(UUID feedId, String content, Instant createdAt) {
        super(feedId);
        this.content = content;
        this.createdAt = createdAt;
    }
}