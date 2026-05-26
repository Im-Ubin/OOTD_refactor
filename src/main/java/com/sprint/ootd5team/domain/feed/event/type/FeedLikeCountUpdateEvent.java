package com.sprint.ootd5team.domain.feed.event.type;

import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
public class FeedLikeCountUpdateEvent extends FeedEvent {

    private final long newLikeCount;

    public FeedLikeCountUpdateEvent(UUID feedId, long newLikeCount) {
        super(feedId);
        this.newLikeCount = newLikeCount;
    }
}
