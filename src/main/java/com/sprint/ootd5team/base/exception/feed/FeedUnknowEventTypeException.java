package com.sprint.ootd5team.base.exception.feed;

import com.sprint.ootd5team.base.errorcode.ErrorCode;

public class FeedUnknowEventTypeException extends FeedException {

    public FeedUnknowEventTypeException() {
        super(ErrorCode.FEED_UNKNOW_EVENT_TYPE);
    }

    public static FeedUnknowEventTypeException withId (String eventType) {
        FeedUnknowEventTypeException exception = new FeedUnknowEventTypeException();
        exception.addDetail("eventType", eventType);
        return exception;
    }
}