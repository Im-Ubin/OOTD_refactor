package com.sprint.ootd5team.domain.feed.event.type;

import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
public class FeedDeletedEvent extends FeedEvent {

    public FeedDeletedEvent(UUID feedId) {
        super(feedId);
    }
}