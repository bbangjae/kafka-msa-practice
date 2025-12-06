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
 * 결제 서비스 - 결제 처리 및 실패 시뮬레이션
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Random random = new Random();

    /**
     * 결제 요청 이벤트 처리
     */
    @KafkaListener(groupId = "payment-service", topics = "payment-events")
    public void handlePaymentEvent(String message) {
        try {
            Map<String, Object> event = objectMapper.readValue(
                message,
                Map.class
            );
            String eventType = (String) event.get("eventType");

            if ("PAYMENT_REQUESTED".equals(eventType)) {
                String orderId = (String) event.get("orderId");
                String customerId = (String) event.get("customerId");
                String amount = event.get("amount").toString();

                processPayment(orderId, customerId, amount);
            }
        } catch (Exception e) {
            log.error("Failed to process payment event", e);
        }
    }

    public void processPayment(
        String orderId,
        String customerId,
        String amount
    ) {
        log.info(
            "[결제 서비스] 주문 {} 결제 처리 시작 (금액: {}원)",
            orderId,
            amount
        );

        // 30% 확률로 결제 실패 시뮬레이션
        boolean paymentSuccess = random.nextInt(100) < 70;

        if (paymentSuccess) {
            log.info("[결제 서비스] 주문 {} 결제 성공", orderId);
            publishPaymentResult(orderId, "PAYMENT_COMPLETED");
        } else {
            log.error("[결제 서비스] 주문 {} 결제 실패", orderId);
            publishPaymentResult(orderId, "PAYMENT_FAILED");
        }
    }

    private void publishPaymentResult(String orderId, String eventType) {
        try {
            Map<String, String> event = Map.of(
                "orderId",
                orderId,
                "eventType",
                eventType
            );
            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("payment-results", orderId, eventJson);
        } catch (Exception e) {
            log.error("Failed to publish payment result", e);
        }
    }
}
