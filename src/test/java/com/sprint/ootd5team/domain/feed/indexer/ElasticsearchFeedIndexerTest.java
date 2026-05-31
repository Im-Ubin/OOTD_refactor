package com.sprint.ootd5team.domain.feed.indexer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.sprint.ootd5team.domain.feed.event.type.FeedContentUpdatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedDeletedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.search.FeedDocument;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
@DisplayName("ElasticsearchFeedIndexer 단위 테스트")
@ActiveProfiles("test")
public class ElasticsearchFeedIndexerTest {

    @Mock
    ElasticsearchOperations operations;

    @InjectMocks
    ElasticsearchFeedIndexer indexer;

    private String indexName;
    private UUID testFeedId;

    @BeforeEach
    void setUp() {
        indexName = "feeds";
        testFeedId = UUID.randomUUID();
        ReflectionTestUtils.setField(indexer, "indexName", indexName);
    }

    @Test
    @DisplayName("피드 생성 이벤트를 받으면 Elasticsearch에 문서 저장")
    void create_savesDocumentToElasticsearch() {
        // given
        String content = "오늘의 OOTD입니다";
        Instant createdAt = Instant.now();
        FeedIndexCreatedEvent event = new FeedIndexCreatedEvent(testFeedId, content, createdAt);

        when(operations.exists(eq(testFeedId.toString()), any(IndexCoordinates.class)))
            .thenReturn(false);
        given(operations.save(any(FeedDocument.class)))
            .willReturn(null);

        // when
        indexer.create(event);

        // then
        ArgumentCaptor<FeedDocument> captor = ArgumentCaptor.forClass(FeedDocument.class);
        verify(operations).save(captor.capture());

        FeedDocument savedDoc = captor.getValue();
        assertThat(savedDoc.getFeedId()).isEqualTo(testFeedId);
        assertThat(savedDoc.getContent()).isEqualTo(content);
        assertThat(savedDoc.getCreatedAt()).isEqualTo(createdAt);
        assertThat(savedDoc.getUpdatedAt()).isEqualTo(createdAt);
    }

    @Test
    @DisplayName("이미 존재하는 문서면 create 스킵")
    void create_documentAlreadyExists_skip() {
        // given
        FeedIndexCreatedEvent event = new FeedIndexCreatedEvent(testFeedId, "내용", Instant.now());

        when(operations.exists(eq(testFeedId.toString()), any(IndexCoordinates.class)))
            .thenReturn(true);

        // when
        indexer.create(event);

        // then
        verify(operations, never()).save(any(FeedDocument.class));
    }

    @Test
    @DisplayName("피드 내용 수정 이벤트를 받으면 content 필드 업데이트")
    void updateContent_updatesContentField() {
        // given
        Instant now = Instant.now();
        String newContent = "수정된 OOTD 내용";
        FeedContentUpdatedEvent event = new FeedContentUpdatedEvent(
            testFeedId, newContent, now.minusSeconds(10), now, 0L);

        when(operations.get(eq(testFeedId.toString()), eq(FeedDocument.class), any(IndexCoordinates.class)))
            .thenReturn(null);

        // when
        indexer.updateContent(event);

        // then
        ArgumentCaptor<UpdateQuery> queryCaptor = ArgumentCaptor.forClass(UpdateQuery.class);
        ArgumentCaptor<IndexCoordinates> indexCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        verify(operations).update(queryCaptor.capture(), indexCaptor.capture());

        UpdateQuery capturedQuery = queryCaptor.getValue();
        assertThat(capturedQuery.getId()).isEqualTo(testFeedId.toString());
        assertThat(capturedQuery.getDocument().get("content")).isEqualTo(newContent);
        assertThat(capturedQuery.getDocument().get("updatedAt")).isEqualTo(now);
        assertThat(indexCaptor.getValue().getIndexName()).isEqualTo(indexName);
    }

    @Test
    @DisplayName("더 최신 이벤트가 이미 반영된 경우 updateContent 스킵")
    void updateContent_outdatedEvent_skip() {
        // given
        Instant now = Instant.now();
        FeedContentUpdatedEvent event = new FeedContentUpdatedEvent(
            testFeedId, "구버전 내용", now.minusSeconds(60), now.minusSeconds(30), 0L);

        FeedDocument existing = FeedDocument.builder()
            .feedId(testFeedId)
            .content("최신 내용")
            .updatedAt(now)
            .build();

        when(operations.get(eq(testFeedId.toString()), eq(FeedDocument.class), any(IndexCoordinates.class)))
            .thenReturn(existing);

        // when
        indexer.updateContent(event);

        // then
        verify(operations, never()).update(any(), any());
    }

    @Test
    @DisplayName("좋아요 수 변경 이벤트를 받으면 likeCount 필드 업데이트")
    void updateLikeCount_updatesLikeCountField() {
        // given
        Instant now = Instant.now();
        long newLikeCount = 42L;
        FeedLikeCountUpdateEvent event = new FeedLikeCountUpdateEvent(
            testFeedId, newLikeCount, now.minusSeconds(10), now, "내용");

        when(operations.get(eq(testFeedId.toString()), eq(FeedDocument.class), any(IndexCoordinates.class)))
            .thenReturn(null);

        // when
        indexer.updateLikeCount(event);

        // then
        ArgumentCaptor<UpdateQuery> queryCaptor = ArgumentCaptor.forClass(UpdateQuery.class);
        ArgumentCaptor<IndexCoordinates> indexCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        verify(operations).update(queryCaptor.capture(), indexCaptor.capture());

        UpdateQuery capturedQuery = queryCaptor.getValue();
        assertThat(capturedQuery.getId()).isEqualTo(testFeedId.toString());
        assertThat(capturedQuery.getDocument().get("likeCount")).isEqualTo(newLikeCount);
        assertThat(capturedQuery.getDocument().get("updatedAt")).isEqualTo(now);
        assertThat(indexCaptor.getValue().getIndexName()).isEqualTo(indexName);
    }

    @Test
    @DisplayName("더 최신 이벤트가 이미 반영된 경우 updateLikeCount 스킵")
    void updateLikeCount_outdatedEvent_skip() {
        // given
        Instant now = Instant.now();
        FeedLikeCountUpdateEvent event = new FeedLikeCountUpdateEvent(
            testFeedId, 10L, now.minusSeconds(60), now.minusSeconds(30), "내용");

        FeedDocument existing = FeedDocument.builder()
            .feedId(testFeedId)
            .likeCount(100L)
            .updatedAt(now)
            .build();

        when(operations.get(eq(testFeedId.toString()), eq(FeedDocument.class), any(IndexCoordinates.class)))
            .thenReturn(existing);

        // when
        indexer.updateLikeCount(event);

        // then
        verify(operations, never()).update(any(), any());
    }

    @Test
    @DisplayName("피드 삭제 이벤트를 받으면 Elasticsearch에서 문서 삭제 실행")
    void delete_removesDocumentFromElasticsearch() {
        // given
        FeedDeletedEvent event = new FeedDeletedEvent(testFeedId);

        // when
        indexer.delete(event);

        // then
        ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<IndexCoordinates> indexCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        verify(operations).delete(idCaptor.capture(), indexCaptor.capture());

        assertThat(idCaptor.getValue()).isEqualTo(testFeedId.toString());
        assertThat(indexCaptor.getValue().getIndexName()).isEqualTo(indexName);
    }

    @Test
    @DisplayName("여러 피드를 연속으로 생성 시 각각 올바르게 인덱싱")
    void create_indexesMultipleFeedsCorrectly() {
        // given
        UUID feedId1 = UUID.randomUUID();
        UUID feedId2 = UUID.randomUUID();
        Instant now = Instant.now();

        FeedIndexCreatedEvent event1 = new FeedIndexCreatedEvent(feedId1, "첫 번째 피드", now);
        FeedIndexCreatedEvent event2 = new FeedIndexCreatedEvent(feedId2, "두 번째 피드", now.plusSeconds(60));

        when(operations.exists(anyString(), any(IndexCoordinates.class)))
            .thenReturn(false);
        given(operations.save(any(FeedDocument.class)))
            .willReturn(null);

        // when
        indexer.create(event1);
        indexer.create(event2);

        // then
        ArgumentCaptor<FeedDocument> captor = ArgumentCaptor.forClass(FeedDocument.class);
        verify(operations, times(2)).save(captor.capture());

        var savedDocs = captor.getAllValues();
        assertThat(savedDocs).hasSize(2);
        assertThat(savedDocs.get(0).getFeedId()).isEqualTo(feedId1);
        assertThat(savedDocs.get(1).getFeedId()).isEqualTo(feedId2);
    }

    @Test
    @DisplayName("같은 피드의 내용과 좋아요수를 순차적으로 업데이트")
    void update_updatesMultipleFieldsSequentially() {
        // given
        Instant now = Instant.now();
        FeedContentUpdatedEvent contentEvent = new FeedContentUpdatedEvent(
            testFeedId, "업데이트된 내용", now.minusSeconds(10), now, 0L);
        FeedLikeCountUpdateEvent likeEvent = new FeedLikeCountUpdateEvent(
            testFeedId, 100L, now.minusSeconds(10), now, "업데이트된 내용");

        when(operations.get(eq(testFeedId.toString()), eq(FeedDocument.class), any(IndexCoordinates.class)))
            .thenReturn(null);

        // when
        indexer.updateContent(contentEvent);
        indexer.updateLikeCount(likeEvent);

        // then
        verify(operations, times(2)).update(any(UpdateQuery.class), any(IndexCoordinates.class));
    }
}