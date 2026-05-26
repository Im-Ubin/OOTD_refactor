package com.sprint.ootd5team.domain.like.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sprint.ootd5team.base.exception.feed.AlreadyLikedException;
import com.sprint.ootd5team.base.exception.feed.FeedNotFoundException;
import com.sprint.ootd5team.base.exception.feed.FeedOutboxSaveFailedException;
import com.sprint.ootd5team.base.exception.feed.LikeCountUnderflowException;
import com.sprint.ootd5team.base.exception.feed.LikeNotFoundException;
import com.sprint.ootd5team.domain.feed.dto.enums.EventStatus;
import com.sprint.ootd5team.domain.feed.entity.Feed;
import com.sprint.ootd5team.domain.feed.entity.FeedEventOutbox;
import com.sprint.ootd5team.domain.feed.event.type.FeedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedOutboxSavedEvent;
import com.sprint.ootd5team.domain.feed.repository.feed.FeedRepository;
import com.sprint.ootd5team.domain.feed.repository.feedOutbox.FeedEventOutboxRepository;
import com.sprint.ootd5team.domain.like.entity.FeedLike;
import com.sprint.ootd5team.domain.like.repository.FeedLikeRepository;
import com.sprint.ootd5team.domain.notification.event.type.single.FeedLikedEvent;
import com.sprint.ootd5team.domain.user.repository.UserRepository;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@RequiredArgsConstructor
@Service
public class FeedLikeServiceImpl implements FeedLikeService {

    private final FeedLikeRepository feedLikeRepository;
    private final FeedRepository feedRepository;
    private final FeedEventOutboxRepository feedEventOutboxRepository;

    private final UserRepository userRepository;
    private final ApplicationEventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    @Transactional
    public void like(UUID feedId, UUID currentUserId) {
        log.info("[FeedLikeService] 피드 좋아요 활성화 시작 - feedId = {}, currentUserId = {}", feedId,
            currentUserId);

        Feed feed = validateFeed(feedId);
        validateNotLiked(feedId, currentUserId);

        FeedLike feedLike = new FeedLike(feedId, currentUserId);
        feedLikeRepository.save(feedLike);
        log.debug("[FeedLikeService] 저장된 FeedLike: {}", feedLike);

        feedRepository.incrementLikeCount(feedId);

        publishLikeCountUpdatedEvent(feedId);

        // 알림 전송
        // 좋아요 누른 사람 이름 가져오기
        String username = userRepository.findUserNameById(currentUserId);
        eventPublisher.publishEvent(
            new FeedLikedEvent(feed.getId(), feed.getAuthorId(), feed.getContent(), username)
        );
    }

    @Transactional
    public void unLike(UUID feedId, UUID currentUserId) {
        log.info("[FeedLikeService] 피드 좋아요 비활성화 시작 - feedId = {}, currentUserId = {}", feedId,
            currentUserId);

        validateFeed(feedId);
        validateLiked(feedId, currentUserId);

        feedLikeRepository.deleteByFeedIdAndUserId(feedId, currentUserId);

        int updatedRows = feedRepository.decrementLikeCount(feedId);
        if (updatedRows == 0) {
            log.error("[FeedLikeService] 좋아요 수 감소 실패");
            throw LikeCountUnderflowException.withFeedId(feedId);
        }

        publishLikeCountUpdatedEvent(feedId);
    }

    private void publishLikeCountUpdatedEvent(UUID feedId) {
        long updatedLikeCount = feedRepository.findLikeCountByFeedId(feedId);

        FeedLikeCountUpdateEvent event = new FeedLikeCountUpdateEvent(feedId, updatedLikeCount);

        saveToOutbox(event, "ootd.Feeds.LikeUpdated", feedId);
    }

    private void saveToOutbox(FeedEvent event, String topic, UUID feedId) {
        try {
            String eventType = event.getClass().getSimpleName();
            String payload = objectMapper.writeValueAsString(event);

            FeedEventOutbox outbox = FeedEventOutbox.builder()
                .aggregateId(feedId)
                .eventType(eventType)
                .topic(topic)
                .payload(payload)
                .status(EventStatus.PENDING)
                .retryCount(0)
                .build();

            feedEventOutboxRepository.save(outbox);

            log.info("[FeedLikeService] Outbox 저장 완료 - outboxId:{}, eventType:{}, feedId:{}",
                outbox.getId(), eventType, feedId);

            eventPublisher.publishEvent(new FeedOutboxSavedEvent(outbox.getId()));

        } catch (Exception e) {
            log.error("[FeedLikeService] Outbox 저장 실패 - {}, feedId:{}", e.getClass().getName(), feedId);
            throw FeedOutboxSaveFailedException.withId(feedId);
        }
    }

    private Feed validateFeed(UUID feedId) {
        return feedRepository.findById(feedId)
            .orElseThrow(() -> {
                log.warn("[FeedLikeService] 피드가 존재하지 않습니다.");
                return FeedNotFoundException.withId(feedId);
            });
    }

    private void validateNotLiked(UUID feedId, UUID currentUserId) {
        boolean exists = feedLikeRepository.existsByFeedIdAndUserId(feedId, currentUserId);
        if (exists) {
            log.warn("[FeedLikeService] 이미 좋아요 처리 된 피드입니다.");
            throw AlreadyLikedException.withIds(feedId, currentUserId);
        }
    }

    private void validateLiked(UUID feedId, UUID currentUserId) {
        if (!feedLikeRepository.existsByFeedIdAndUserId(feedId, currentUserId)) {
            log.warn("[FeedLikeService] 존재하지 않는 좋아요입니다.");
            throw LikeNotFoundException.withIds(feedId, currentUserId);
        }
    }
}