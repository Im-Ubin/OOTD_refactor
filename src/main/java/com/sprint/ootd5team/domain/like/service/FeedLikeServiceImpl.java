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
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 피드 좋아요 서비스 구현체
 *
 * <p>좋아요 등록/취소 시 DB 업데이트와 함께
 * Outbox Pattern을 통해 Kafka 이벤트를 발행한다.</p>
 */
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

    /**
     * 피드 좋아요를 등록한다.
     *
     * <p>좋아요 등록 후 likeCount를 증가시키고,
     * Outbox를 통해 {@link FeedLikeCountUpdateEvent}를 발행한다.
     * 또한 피드 작성자에게 좋아요 알림 이벤트를 발행한다.</p>
     *
     * @param feedId        좋아요할 피드 ID
     * @param currentUserId 현재 로그인 사용자 ID
     * @throws FeedNotFoundException  피드가 존재하지 않을 경우
     * @throws AlreadyLikedException  이미 좋아요한 피드인 경우
     */
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

        publishLikeCountUpdatedEvent(feed);

        // 알림 전송
        // 좋아요 누른 사람 이름 가져오기
        String username = userRepository.findUserNameById(currentUserId);
        eventPublisher.publishEvent(
            new FeedLikedEvent(feed.getId(), feed.getAuthorId(), feed.getContent(), username)
        );
    }

    /**
     * 피드 좋아요를 취소한다.
     *
     * <p>좋아요 취소 후 likeCount를 감소시키고,
     * Outbox를 통해 {@link FeedLikeCountUpdateEvent}를 발행한다.</p>
     *
     * @param feedId        좋아요를 취소할 피드 ID
     * @param currentUserId 현재 로그인 사용자 ID
     * @throws FeedNotFoundException      피드가 존재하지 않을 경우
     * @throws LikeNotFoundException      좋아요가 존재하지 않을 경우
     * @throws LikeCountUnderflowException likeCount 감소 실패 시
     */
    @Transactional
    public void unLike(UUID feedId, UUID currentUserId) {
        log.info("[FeedLikeService] 피드 좋아요 비활성화 시작 - feedId = {}, currentUserId = {}", feedId,
            currentUserId);

        Feed feed = validateFeed(feedId);
        validateLiked(feedId, currentUserId);

        feedLikeRepository.deleteByFeedIdAndUserId(feedId, currentUserId);

        int updatedRows = feedRepository.decrementLikeCount(feedId);
        if (updatedRows == 0) {
            log.error("[FeedLikeService] 좋아요 수 감소 실패");
            throw LikeCountUnderflowException.withFeedId(feedId);
        }

        publishLikeCountUpdatedEvent(feed);
    }

    /**
     * 좋아요 수 변경 이벤트를 Outbox에 저장한다.
     *
     * <p>DB에서 최신 likeCount를 조회하여 이벤트를 생성하고,
     * ES 순서 역전 대응을 위해 createdAt, updatedAt, content를 함께 포함한다.</p>
     *
     * @param feed 좋아요가 변경된 피드 엔티티
     */
    private void publishLikeCountUpdatedEvent(Feed feed) {
        UUID feedId = feed.getId();
        long updatedLikeCount = feedRepository.findLikeCountByFeedId(feedId);

        FeedLikeCountUpdateEvent event = new FeedLikeCountUpdateEvent(
            feedId,
            updatedLikeCount,
            feed.getCreatedAt(),
            Instant.now(),
            feed.getContent()
        );

        saveToOutbox(event, "ootd.Feeds.LikeUpdated", feedId);
    }

    /**
     * 이벤트를 Outbox 테이블에 저장하고 발행 트리거 이벤트를 발행한다.
     *
     * @param event  저장할 피드 이벤트
     * @param topic  Kafka 토픽
     * @param feedId Feed ID
     * @throws FeedOutboxSaveFailedException Outbox 저장 실패 시
     */
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

    /**
     * 피드 존재 여부를 검증하고 반환한다.
     *
     * @param feedId 검증할 피드 ID
     * @return 조회된 Feed 엔티티
     * @throws FeedNotFoundException 피드가 존재하지 않을 경우
     */
    private Feed validateFeed(UUID feedId) {
        return feedRepository.findById(feedId)
            .orElseThrow(() -> {
                log.warn("[FeedLikeService] 피드가 존재하지 않습니다.");
                return FeedNotFoundException.withId(feedId);
            });
    }

    /**
     * 이미 좋아요한 피드인지 검증한다.
     *
     * @param feedId        피드 ID
     * @param currentUserId 현재 로그인 사용자 ID
     * @throws AlreadyLikedException 이미 좋아요한 경우
     */
    private void validateNotLiked(UUID feedId, UUID currentUserId) {
        boolean exists = feedLikeRepository.existsByFeedIdAndUserId(feedId, currentUserId);
        if (exists) {
            log.warn("[FeedLikeService] 이미 좋아요 처리 된 피드입니다.");
            throw AlreadyLikedException.withIds(feedId, currentUserId);
        }
    }

    /**
     * 좋아요가 존재하는지 검증한다.
     *
     * @param feedId        피드 ID
     * @param currentUserId 현재 로그인 사용자 ID
     * @throws LikeNotFoundException 좋아요가 존재하지 않는 경우
     */
    private void validateLiked(UUID feedId, UUID currentUserId) {
        if (!feedLikeRepository.existsByFeedIdAndUserId(feedId, currentUserId)) {
            log.warn("[FeedLikeService] 존재하지 않는 좋아요입니다.");
            throw LikeNotFoundException.withIds(feedId, currentUserId);
        }
    }
}