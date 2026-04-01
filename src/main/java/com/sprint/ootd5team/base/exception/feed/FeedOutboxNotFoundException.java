package com.sprint.ootd5team.base.exception.feed;

import com.sprint.ootd5team.base.errorcode.ErrorCode;

public class FeedOutboxNotFoundException extends FeedException {

    public FeedOutboxNotFoundException() {
        super(ErrorCode.FEED_OUTBOX_NOT_FOUND);
    }

    public static FeedOutboxNotFoundException withId (Long outboxId) {
        FeedOutboxNotFoundException exception = new FeedOutboxNotFoundException();
        exception.addDetail("outboxId", outboxId);
        return exception;
    }
}