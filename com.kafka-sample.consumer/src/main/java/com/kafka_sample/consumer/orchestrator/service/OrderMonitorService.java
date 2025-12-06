package com.kafka_sample.consumer.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * 주문 이벤트 모니터링 서비스
 * 주문 생성, 완료, 취소 등의 이벤트를 로깅
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderMonitorService {

    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 주문 이벤트 로깅 (모니터링용)
     */
    @KafkaListener(groupId = "order-monitor", topics = "order-events")
    public void handleOrderEvent(String message) {
        try {
            Map<String, Object> event = objectMapper.readValue(
                message,
                Map.class
            );
            String eventType = (String) event.get("eventType");
            String orderId = (String) event.get("orderId");

            String eventMessage = switch (eventType) {
                case "ORDER_CREATED" -> "[주문 모니터] 주문 {} 생성됨";
                case "ORDER_COMPLETED" -> "[주문 모니터] 주문 {} 완료됨";
                case "ORDER_CANCELLED" -> "[주문 모니터] 주문 {} 취소됨";
                default -> "[주문 모니터] 주문 {} 이벤트: " + eventType;
            };

            log.info(eventMessage, orderId);
        } catch (Exception e) {
            log.error("주문 이벤트 처리 실패", e);
        }
    }
}
