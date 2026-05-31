package com.sprint.ootd5team.domain.feed.event;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.sprint.ootd5team.domain.feed.event.consumer.FeedEventConsumer;
import com.sprint.ootd5team.domain.feed.event.type.FeedContentUpdatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedDeletedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedIndexCreatedEvent;
import com.sprint.ootd5team.domain.feed.event.type.FeedLikeCountUpdateEvent;
import com.sprint.ootd5team.domain.feed.indexer.ElasticsearchFeedIndexer;
import com.sprint.ootd5team.domain.feed.repository.feed.FeedRepository;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@DisplayName("FeedEventConsumer 슬라이스 테스트")
public class FeedEventConsumerTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private ElasticsearchFeedIndexer indexer;

    @Mock
    private FeedRepository feedRepository;

    @InjectMocks
    private FeedEventConsumer consumer;

    private String payload;
    private UUID feedId;

    @BeforeEach
    void setUp() {
        feedId = UUID.randomUUID();
        payload = "{\"feedId\":\"" + feedId + "\"}";
    }

    @Test
    @DisplayName("FeedIndexCreatedEvent 수신 시 DB에 존재하면 인덱서 호출")
    void consumeFeedIndexCreatedEvent_success() throws Exception {
        // given
        FeedIndexCreatedEvent event = new FeedIndexCreatedEvent(feedId, "내용", Instant.now());

        when(objectMapper.readValue(payload, FeedIndexCreatedEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId))
            .thenReturn(true);

        // when
        consumer.consumeFeedIndexCreatedEvent(payload);

        // then
        verify(indexer).create(event);
    }

    @Test
    @DisplayName("FeedIndexCreatedEvent 수신 시 DB에 없으면 인덱서 호출 안 함")
    void consumeFeedIndexCreatedEvent_feedNotExists_skip() throws Exception {
        // given
        FeedIndexCreatedEvent event = new FeedIndexCreatedEvent(feedId, "내용", Instant.now());

        when(objectMapper.readValue(payload, FeedIndexCreatedEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId))
            .thenReturn(false);

        // when
        consumer.consumeFeedIndexCreatedEvent(payload);

        // then
        verify(indexer, never()).create(any());
    }

    @Test
    @DisplayName("FeedContentUpdatedEvent 수신 시 DB에 존재하면 ES update 호출")
    void consumeFeedContentUpdatedEvent_success() throws Exception {
        // given
        FeedContentUpdatedEvent event = new FeedContentUpdatedEvent(
            feedId, "새로운 내용", Instant.now(), Instant.now(), 0L);

        when(objectMapper.readValue(payload, FeedContentUpdatedEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId))
            .thenReturn(true);

        // when
        consumer.consumeFeedContentUpdatedEvent(payload);

        // then
        verify(indexer).updateContent(event);
    }

    @Test
    @DisplayName("FeedContentUpdatedEvent 수신 시 DB에 없으면 ES update 호출 안 함")
    void consumeFeedContentUpdatedEvent_feedNotExists_skip() throws Exception {
        // given
        FeedContentUpdatedEvent event = new FeedContentUpdatedEvent(
            feedId, "새로운 내용", Instant.now(), Instant.now(), 0L);

        when(objectMapper.readValue(payload, FeedContentUpdatedEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId)).thenReturn(false);

        // when
        consumer.consumeFeedContentUpdatedEvent(payload);

        // then
        verify(indexer, never()).updateContent(any());
    }

    @Test
    @DisplayName("FeedLikeCountUpdateEvent 수신 시 DB에 존재하면 ES update 호출")
    void consumeFeedLikeCountUpdatedEvent_success() throws Exception {
        // given
        FeedLikeCountUpdateEvent event = new FeedLikeCountUpdateEvent(
            feedId, 42L, Instant.now(), Instant.now(), "내용");

        when(objectMapper.readValue(payload, FeedLikeCountUpdateEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId)).thenReturn(true);

        // when
        consumer.consumeFeedLikeCountUpdatedEvent(payload);

        // then
        verify(indexer).updateLikeCount(event);
    }

    @Test
    @DisplayName("FeedDeletedEvent 수신 시 DB 체크 없이 ES delete 호출")
    void consumeFeedDeletedEvent_success() throws Exception {
        // given
        FeedDeletedEvent event = new FeedDeletedEvent(feedId);

        when(objectMapper.readValue(payload, FeedDeletedEvent.class))
            .thenReturn(event);

        // when
        consumer.consumeFeedDeletedEvent(payload);

        // then
        verify(indexer).delete(event);
        verify(feedRepository, never()).existsById(any());
    }

    @Test
    @DisplayName("역직렬화 실패 시 RuntimeException 발생")
    void handleEvent_deserializationFailure_throwsException() throws Exception {
        // when
        when(objectMapper.readValue(payload, FeedIndexCreatedEvent.class))
            .thenThrow(new RuntimeException("역직렬화 실패"));

        // then
        assertThatThrownBy(() -> consumer.consumeFeedIndexCreatedEvent(payload))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Kafka 메시지 처리 실패");

        verify(indexer, never()).create(any());
    }

    @Test
    @DisplayName("이중 직렬화 감지 시 unwrap 후 재역직렬화")
    void handleEvent_doubleEncodedJson_unwrapAndProcess() throws Exception {
        // given
        String unwrapped = "{\"feedId\":\"" + feedId + "\",\"content\":\"내용\",\"createdAt\":\"" + Instant.now() + "\"}";
        String doubleEncoded = "\"" + unwrapped.replace("\"", "\\\"") + "\"";

        FeedIndexCreatedEvent event =
            new FeedIndexCreatedEvent(feedId, "내용", Instant.now());

        when(objectMapper.readValue(doubleEncoded, FeedIndexCreatedEvent.class))
            .thenThrow(mock(MismatchedInputException.class));
        when(objectMapper.readValue(doubleEncoded, String.class))
            .thenReturn(unwrapped);
        when(objectMapper.readValue(unwrapped, FeedIndexCreatedEvent.class))
            .thenReturn(event);
        when(feedRepository.existsById(feedId))
            .thenReturn(true);

        // when
        consumer.consumeFeedIndexCreatedEvent(doubleEncoded);

        // then
        verify(indexer).create(event);
    }

    @Test
    @DisplayName("MismatchedInputException이지만 이중 직렬화 아닌 경우 실패 처리")
    void handleEvent_mismatchedButNotDoubleEncoded_throwsException() throws Exception {
        // when
        when(objectMapper.readValue(payload, FeedIndexCreatedEvent.class))
            .thenThrow(mock(MismatchedInputException.class));

        // then
        assertThatThrownBy(() -> consumer.consumeFeedIndexCreatedEvent(payload))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Kafka 메시지 처리 실패");

        verify(indexer, never()).create(any());
    }

    @Test
    @DisplayName("ES 처리 중 예외 발생 시 RuntimeException 전파")
    void handleEvent_esException_throwsException() throws Exception {
        // given
        FeedDeletedEvent event = new FeedDeletedEvent(feedId);

        when(objectMapper.readValue(payload, FeedDeletedEvent.class))
            .thenReturn(event);
        doThrow(new RuntimeException("ES error"))
            .when(indexer).delete(event);

        // then
        assertThatThrownBy(() -> consumer.consumeFeedDeletedEvent(payload))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Kafka 메시지 처리 실패");
    }
}
