package com.sprint.ootd5team.domain.feed.event.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.sprint.ootd5team.domain.feed.event.type.FeedContentUpdatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedDeletedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.indexer.ElasticsearchFeedIndexer;
import com.sprint.ootd5team.domain.feed.repository.feed.FeedRepository;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka에서 발행된 피드 관련 이벤트를 수신하고,
 * 해당 이벤트 타입에 따라 {@link ElasticsearchFeedIndexer}를 통해 인덱스를 갱신하는 소비자
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FeedEventConsumer {

    private final ObjectMapper objectMapper;
    private final ElasticsearchFeedIndexer indexer;
    private final FeedRepository feedRepository;

    @KafkaListener(topics = "ootd.Feeds.Created", groupId = "ootd.feed-indexer")
    public void consumeFeedIndexCreatedEvent(String message) {
        handleEvent(message, FeedIndexCreatedEvent.class, indexer::create, true);
    }

    @KafkaListener(topics = "ootd.Feeds.ContentUpdated", groupId = "ootd.feed-indexer")
    public void consumeFeedContentUpdatedEvent(String message) {
        handleEvent(message, FeedContentUpdatedEvent.class, indexer::updateContent, true);
    }

    @KafkaListener(topics = "ootd.Feeds.LikeUpdated", groupId = "ootd.feed-indexer")
    public void consumeFeedLikeCountUpdatedEvent(String message) {
        handleEvent(message, FeedLikeCountUpdateEvent.class, indexer::updateLikeCount, true);
    }

    @KafkaListener(topics = "ootd.Feeds.Deleted", groupId = "ootd.feed-indexer")
    public void consumeFeedDeletedEvent(String message) {
        handleEvent(message, FeedDeletedEvent.class, indexer::delete, false);
    }

    /**
     * 전달받은 Kafka 메시지를 지정된 이벤트 타입으로 역직렬화하고,
     * 해당 이벤트에 맞는 인덱싱 작업을 수행한다.
     *
     * <p>이중 직렬화된 메시지가 감지되면 언래핑 후 재역직렬화를 시도한다.</p>
     * <p>checkExists가 true인 경우, DB에서 피드 존재 여부를 확인하여
     *  * 이미 삭제된 피드에 대한 이벤트는 처리를 스킵한다.</p>
     *
     * @param message 수신한 Kafka 메시지(JSON 문자열)
     * @param clazz 역직렬화할 이벤트 클래스 타입
     * @param checkExists  true인 경우 DB에서 피드 존재 여부를 확인 후 처리
     * @param handler 이벤트 처리 로직을 수행할 함수
     * @param <T> {@link FeedEvent}를 상속한 이벤트 타입
     * @throws RuntimeException Kafka 메시지 처리 중 예외 발생 시
     */
    private <T extends FeedEvent> void handleEvent(String message, Class<T> clazz, Consumer<T> handler, boolean checkExists) {
        try {
            T event;
            try {
                event = objectMapper.readValue(message, clazz);
            } catch (MismatchedInputException e) {
                if (looksLikeDoubleEncoded(message)) {
                    String unwrapped = objectMapper.readValue(message, String.class);
                    log.warn("[FeedEventConsumer] 이중 직렬화 감지. clazz={}", clazz.getSimpleName());
                    event = objectMapper.readValue(unwrapped, clazz);
                } else {
                    throw e;
                }
            }

            if (checkExists && !feedRepository.existsById(event.getFeedId())) {
                log.warn("[FeedEventConsumer] 이미 삭제된 피드 - feedId: {}", event.getFeedId());
                return;
            }

            handler.accept(event);
            log.info("[FeedEventConsumer] {} 처리 완료: {}", clazz.getSimpleName(), event);

        } catch (Exception e) {
            throw new RuntimeException("[FeedEventConsumer] Kafka 메시지 처리 실패", e);
        }
    }

    private boolean looksLikeDoubleEncoded(String s) {
        if (s == null) return false;
        String trimmed = s.trim();

        return trimmed.length() >= 2
            && trimmed.charAt(0) == '"'
            && trimmed.charAt(trimmed.length() - 1) == '"'
            && trimmed.contains("\\\"feedId\\\"");
    }
}