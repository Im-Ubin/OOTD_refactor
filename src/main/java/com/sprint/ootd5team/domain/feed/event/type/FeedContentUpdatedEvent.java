package com.sprint.ootd5team.domain.feed.event.type;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.UUID;
import lombok.Getter;
import lombok.ToString;

@ToString(callSuper = true)
@Getter
@JsonTypeName("feed-updated")
public class FeedContentUpdatedEvent extends FeedEvent {

    private final String content;

    public FeedContentUpdatedEvent(UUID feedId, String content) {
        super(feedId);
        this.content = content;
    }
}