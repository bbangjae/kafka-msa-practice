package com.kafka_sample.consumer.simple;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ConsumerEndpoint {

    @KafkaListener(groupId = "group_a", topics = "topic1")
    public void consumeFromGroupA(String message) {
        log.info("그룹 A topic1에서 메시지 수신: {}", message);
    }

    @KafkaListener(groupId = "group_b", topics = "topic1")
    public void consumeFromGroupB(String message) {
        log.info("그룹 B topic1에서 메시지 수신: {}", message);
    }

    @KafkaListener(groupId = "group_c", topics = "topic2")
    public void consumeFromGroupC(String message) {
        log.info("그룹 C topic2에서 메시지 수신: {}", message);
    }

    @KafkaListener(groupId = "group_d", topics = "topic3")
    public void consumeFromGroupD(String message) {
        log.info("그룹 D topic3에서 메시지 수신: {}", message);
    }

    @KafkaListener(groupId = "group_d", topics = "topic4")
    public void consumeFromGroupD2(String message) {
        log.info("그룹 D topic4에서 메시지 수신: {}", message);
    }
}
