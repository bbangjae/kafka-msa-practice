package com.kafka_sample.consumer.orchestrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Random;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * 재고 서비스 - 재고 예약 및 실패 시뮬레이션
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    /**
     * 재고 예약 이벤트 처리
     */
    @KafkaListener(groupId = "inventory-service", topics = "inventory-events")
    public void handleInventoryEvent(String message) {
        try {
            Map<String, Object> event = objectMapper.readValue(
                message,
                Map.class
            );
            String eventType = (String) event.get("eventType");

            if ("INVENTORY_RESERVED".equals(eventType)) {
                String orderId = (String) event.get("orderId");
                String productId = (String) event.get("productId");
                Integer quantity = (Integer) event.get("quantity");

                reserveInventory(orderId, productId, quantity);
            }
        } catch (Exception e) {
            log.error("Failed to process inventory event", e);
        }
    }

    public void reserveInventory(
        String orderId,
        String productId,
        Integer quantity
    ) {
        log.info(
            "[재고 서비스] 주문 {} 재고 예약 시작 (상품: {}, 수량: {}개)",
            orderId,
            productId,
            quantity
        );

        // 20% 확률로 재고 부족 시뮬레이션
        boolean inventoryAvailable = random.nextInt(100) < 80;

        if (inventoryAvailable) {
            log.info("[재고 서비스] 주문 {} 재고 예약 완료", orderId);
            publishInventoryResult(orderId, "INVENTORY_RESERVED");
        } else {
            log.error("[재고 서비스] 주문 {} 재고 부족", orderId);
            publishInventoryResult(orderId, "INVENTORY_FAILED");
        }
    }

    private void publishInventoryResult(String orderId, String eventType) {
        try {
            Map<String, String> event = Map.of(
                "orderId",
                orderId,
                "eventType",
                eventType
            );
            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("inventory-results", orderId, eventJson);
        } catch (Exception e) {
            log.error("Failed to publish inventory result", e);
        }
    }
}
