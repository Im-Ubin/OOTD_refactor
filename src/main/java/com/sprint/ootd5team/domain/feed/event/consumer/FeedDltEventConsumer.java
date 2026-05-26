package com.sprint.ootd5team.domain.feed.event.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedDltEventConsumer {

    private final JavaMailSender mailSender;

    @Value("${ootd.email.sender}")
    private String sender;

    @Value("${ootd.admin.email}")
    private String adminEmail;

    @KafkaListener(
        topics = {
            "ootd.Feeds.Created.DLT",
            "ootd.Feeds.ContentUpdated.DLT",
            "ootd.Feeds.LikeUpdated.DLT",
            "ootd.Feeds.Deleted.DLT"
        },
        groupId = "ootd.feed-indexer.dlt"
    )
    public void consumeDLT(ConsumerRecord<String, String> record) {
        log.error("[FeedDltEventListener] DLT 메시지 수신 - topic: {}, message: {}",
            record.topic(), record.value());

        try {
            sendFailureAlert(record);
        } catch (Exception e) {
            log.error("[FeedDltEventListener] 알림 이메일 발송 실패 - topic: {}",
                record.topic(), e);
        }
    }

    private void sendFailureAlert(ConsumerRecord<String, String> record) {
        SimpleMailMessage message = new SimpleMailMessage();
        message.setFrom(sender);
        message.setTo(adminEmail);
        message.setSubject("[OOTD] Feed 이벤트 처리 실패 (DLT)");
        message.setText(
            "Feed 이벤트 처리가 최대 재시도 횟수를 초과했습니다.\n\n"
                + "topic: " + record.topic() + "\n"
                + "message: " + record.value()
        );
        mailSender.send(message);
    }
}