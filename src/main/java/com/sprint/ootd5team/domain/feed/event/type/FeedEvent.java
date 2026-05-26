package com.sprint.ootd5team.domain.feed.event.type;

import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public abstract class FeedEvent {
    private final UUID feedId;

    protected FeedEvent(UUID feedId) {
        this.feedId = feedId;
    }
}