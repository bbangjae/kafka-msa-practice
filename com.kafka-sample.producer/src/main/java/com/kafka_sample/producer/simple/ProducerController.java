package com.kafka_sample.producer.simple;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/messages")
public class ProducerController {

    private final ProducerService producerService;

    @PostMapping("/{topic}")
    public ResponseEntity<String> sendMessage(
        @PathVariable String topic,
        @RequestBody MessageDto messageDto
    ) {
        producerService.sendMessage(
            topic,
            messageDto.getKey(),
            messageDto.getMessage()
        );

        return ResponseEntity.ok("메시지가 Kafka 토픽으로 전송 완료");
    }
}
