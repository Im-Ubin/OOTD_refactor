package com.sprint.ootd5team.domain.feed.indexer;

import com.sprint.ootd5team.domain.feed.event.type.FeedContentUpdatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedDeletedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.search.FeedDocument;
import java.time.Instant;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Component;

/**
 * Kafka로부터 전달받은 피드 관련 이벤트를 기반으로
 * Elasticsearch 인덱스를 생성·수정·삭제하는 컴포넌트
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ElasticsearchFeedIndexer {

    private final ElasticsearchOperations operations;

    @Value("${spring.elasticsearch.indices.feed}")
    private String indexName;

    /**
     * 피드 생성 이벤트를 기반으로 새로운 Elasticsearch 문서를 생성합니다.
     *
     * 이미 문서가 존재하면 스킵됩니다.
     */
    public void create(FeedIndexCreatedEvent event) {
        String feedId = event.getFeedId().toString();

        if (operations.exists(feedId, IndexCoordinates.of(indexName))) {
            log.warn("[ElasticsearchFeedIndexer] 이미 존재하는 문서 - feedId: {}", feedId);
            return;
        }

        FeedDocument document = FeedDocument.builder()
            .feedId(event.getFeedId())
            .content(event.getContent())
            .createdAt(event.getCreatedAt())
            .updatedAt(event.getCreatedAt())
            .build();

        operations.save(document);

        log.info("[ElasticsearchFeedIndexer] Feed 인덱싱 완료: {}", event.getFeedId());
    }

    /**
     * 피드 내용 수정 이벤트를 기반으로 Elasticsearch 문서의 content 필드를 업데이트합니다.
     *
     * 이미 더 최신 이벤트가 반영된 경우 스킵됩니다.
     */
    public void updateContent(FeedContentUpdatedEvent event) {
        if (isOutdated(event.getFeedId().toString(), event.getUpdatedAt())) {
            return;
        }

        Map<String, Object> doc = Map.of(
            "content", event.getContent(),
            "createdAt", event.getCreatedAt(),
            "likeCount", event.getLikeCount(),
            "updatedAt", event.getUpdatedAt()
        );

        UpdateQuery query = UpdateQuery.builder(event.getFeedId().toString())
            .withDocument(Document.from(doc))
            .withUpsert(Document.from(doc))
            .build();

        operations.update(query, IndexCoordinates.of(indexName));
        log.info("[ElasticsearchFeedIndexer] content 업데이트 완료: {}", event.getFeedId());
    }

    /**
     * 좋아요 수 변경 이벤트를 기반으로 Elasticsearch 문서의 likeCount 필드를 업데이트합니다.
     *
     * 이미 더 최신 이벤트가 반영된 경우 스킵됩니다.
     */
    public void updateLikeCount(FeedLikeCountUpdateEvent event) {
        if (isOutdated(event.getFeedId().toString(), event.getUpdatedAt())) {
            return;
        }

        Map<String, Object> doc = Map.of(
            "likeCount", event.getNewLikeCount(),
            "createdAt", event.getCreatedAt(),
            "content", event.getContent(),
            "updatedAt", event.getUpdatedAt()
        );

        UpdateQuery query = UpdateQuery.builder(event.getFeedId().toString())
            .withDocument(Document.from(doc))
            .withUpsert(Document.from(doc))
            .build();

        operations.update(query, IndexCoordinates.of(indexName));
        log.info("[ElasticsearchFeedIndexer] likeCount 업데이트 완료: {}", event.getFeedId());
    }

    /**
     * 피드 삭제 이벤트를 기반으로 Elasticsearch 인덱스에서 해당 문서를 제거합니다.
     */
    public void delete(FeedDeletedEvent event) {
        operations.delete(event.getFeedId().toString(), IndexCoordinates.of(indexName));
        log.info("[ElasticsearchFeedIndexer] 인덱스 삭제 완료: {}", event.getFeedId());
    }

    /**
     * 현재 ES 문서의 updatedAt이 이벤트의 updatedAt보다 최신인지 확인합니다.
     *
     * @param feedId    피드 ID
     * @param eventUpdatedAt 이벤트의 updatedAt
     * @return 오래된 이벤트면 true
     */
    private boolean isOutdated(String feedId, Instant eventUpdatedAt) {
        FeedDocument existing = operations.get(
            feedId,
            FeedDocument.class,
            IndexCoordinates.of(indexName)
        );

        if (existing != null && existing.getUpdatedAt() != null
            && existing.getUpdatedAt().isAfter(eventUpdatedAt)) {
            log.warn("[ElasticsearchFeedIndexer] 오래된 이벤트 무시 - feedId: {}", feedId);
            return true;
        }
        return false;
    }
}