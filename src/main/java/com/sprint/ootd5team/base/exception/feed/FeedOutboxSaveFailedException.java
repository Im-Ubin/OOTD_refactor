package com.sprint.ootd5team.base.exception.feed;

import com.sprint.ootd5team.base.errorcode.ErrorCode;
import java.util.UUID;

public class FeedOutboxSaveFailedException extends FeedException {

    public FeedOutboxSaveFailedException() {
        super(ErrorCode.FEED_OUTBOX_SAVE_FAILED_EXCEPTION);
    }

    public static FeedOutboxSaveFailedException withId(UUID feedId) {
        FeedOutboxSaveFailedException exception = new FeedOutboxSaveFailedException();
        exception.addDetail("feedId", feedId.toString());
        return exception;
    }
}
