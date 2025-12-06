package com.kafka_sample.producer.orchestrator.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka_sample.producer.orchestrator.saga.OrderSagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Saga 실행 결과를 수신하여 Orchestrator에게 전달
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaResultListener {

    private final OrderSagaOrchestrator orchestrator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 결제 결과 수신
     */
    @KafkaListener(groupId = "saga-orchestrator", topics = "payment-results")
    public void handlePaymentResult(String message) {
        try {
            Map<String, String> result = objectMapper.readValue(message, Map.class);
            String orderId = result.get("orderId");
            String eventType = result.get("eventType");

            if ("PAYMENT_COMPLETED".equals(eventType)) {
                orchestrator.handlePaymentCompleted(orderId);
            } else if ("PAYMENT_FAILED".equals(eventType)) {
                orchestrator.handlePaymentFailed(orderId);
            }
        } catch (Exception e) {
            log.error("Failed to process payment result", e);
        }
    }

    /**
     * 재고 결과 수신
     */
    @KafkaListener(groupId = "saga-orchestrator", topics = "inventory-results")
    public void handleInventoryResult(String message) {
        try {
            Map<String, String> result = objectMapper.readValue(message, Map.class);
            String orderId = result.get("orderId");
            String eventType = result.get("eventType");

            if ("INVENTORY_RESERVED".equals(eventType)) {
                orchestrator.handleInventoryReserved(orderId);
            } else if ("INVENTORY_FAILED".equals(eventType)) {
                orchestrator.handleInventoryFailed(orderId);
            }
        } catch (Exception e) {
            log.error("Failed to process inventory result", e);
        }
    }
}
