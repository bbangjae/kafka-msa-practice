package com.kafka_sample.producer.orchestrator.saga;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka_sample.producer.orchestrator.domain.Order;
import com.kafka_sample.producer.orchestrator.domain.OrderStatus;
import com.kafka_sample.producer.orchestrator.event.OrderEvent;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Orchestration-based Saga 패턴
 * 주문 -> 결제 -> 재고 프로세스를 조율하고, 실패 시 보상 트랜잭션 실행
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderSagaOrchestrator {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // 진행 중인 Saga 상태 저장
    private final Map<String, Order> sagaState = new ConcurrentHashMap<>();

    /**
     * Saga 시작: 주문 생성
     */
    public void startOrderSaga(Order order) {
        log.info(
            "[Saga 시작] 주문 {} - Saga 프로세스 시작",
            order.getOrderId()
        );

        order.setStatus(OrderStatus.PENDING);
        sagaState.put(order.getOrderId(), order);

        // Step 1: 주문 생성 이벤트 발행
        publishEvent(
            "order-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .amount(order.getAmount())
                .customerId(order.getCustomerId())
                .eventType(OrderEvent.EventType.ORDER_CREATED)
                .build()
        );

        // Step 2: 결제 요청
        requestPayment(order);

        // Step 3: 재고
        requestPayment(order);
    }

    /**
     * Step 2: 결제 요청
     */
    private void requestPayment(Order order) {
        log.info("[Saga 단계 2] 주문 {} - 결제 요청", order.getOrderId());

        order.setStatus(OrderStatus.PAYMENT_PENDING);

        publishEvent(
            "payment-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .amount(order.getAmount())
                .customerId(order.getCustomerId())
                .eventType(OrderEvent.EventType.PAYMENT_REQUESTED)
                .build()
        );
    }

    /**
     * Step 3: 결제 완료 처리
     */
    public void handlePaymentCompleted(String orderId) {
        Order order = sagaState.get(orderId);
        if (order == null) {
            log.warn(
                "[Saga 상태 없음] 주문 {} - 이미 완료되었거나 존재하지 않는 주문",
                orderId
            );
            return;
        }

        log.info("[Saga 단계 3] 주문 {} - 결제 완료", orderId);
        order.setStatus(OrderStatus.PAYMENT_COMPLETED);

        // Step 4: 재고 예약 요청
        requestInventoryReservation(order);
    }

    /**
     * Step 4: 재고 예약 요청
     */
    private void requestInventoryReservation(Order order) {
        log.info(
            "[Saga 단계 4] 주문 {} - 재고 예약 요청",
            order.getOrderId()
        );

        publishEvent(
            "inventory-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .productId(order.getProductId())
                .quantity(order.getQuantity())
                .eventType(OrderEvent.EventType.INVENTORY_RESERVED)
                .build()
        );
    }

    /**
     * Step 5: 재고 예약 완료 -> 주문 완료
     */
    public void handleInventoryReserved(String orderId) {
        Order order = sagaState.get(orderId);
        if (order == null) {
            log.warn(
                "[Saga 상태 없음] 주문 {} - 이미 완료되었거나 존재하지 않는 주문",
                orderId
            );
            return;
        }

        log.info("[Saga 단계 5] 주문 {} - 재고 예약 완료", orderId);
        order.setStatus(OrderStatus.COMPLETED);

        publishEvent(
            "order-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .eventType(OrderEvent.EventType.ORDER_COMPLETED)
                .build()
        );

        log.info("[Saga 완료] 주문 {} - 정상 완료", orderId);
        sagaState.remove(orderId);
    }

    /**
     * 보상 트랜잭션: 결제 실패 시
     */
    public void handlePaymentFailed(String orderId) {
        Order order = sagaState.get(orderId);
        if (order == null) {
            log.error("Saga 상태에서 주문을 찾을 수 없음: {}", orderId);
            return;
        }

        log.warn(
            "[보상 트랜잭션] 주문 {} - 결제 실패로 보상 트랜잭션 시작",
            orderId
        );
        cancelOrder(order);
    }

    /**
     * 보상 트랜잭션: 재고 예약 실패 시
     */
    public void handleInventoryFailed(String orderId) {
        Order order = sagaState.get(orderId);
        if (order == null) {
            log.error("Saga 상태에서 주문을 찾을 수 없음: {}", orderId);
            return;
        }

        log.warn(
            "[보상 트랜잭션] 주문 {} - 재고 부족으로 보상 트랜잭션 시작",
            orderId
        );

        // 보상 1: 결제 환불
        refundPayment(order);

        // 보상 2: 주문 취소
        cancelOrder(order);
    }

    /**
     * 보상 트랜잭션: 결제 환불
     */
    private void refundPayment(Order order) {
        log.info("[보상] 주문 {} - 결제 환불 처리", order.getOrderId());

        publishEvent(
            "payment-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .amount(order.getAmount())
                .customerId(order.getCustomerId())
                .eventType(OrderEvent.EventType.PAYMENT_FAILED)
                .build()
        );
    }

    /**
     * 보상 트랜잭션: 주문 취소
     */
    private void cancelOrder(Order order) {
        log.info("[보상] 주문 {} - 주문 취소 처리", order.getOrderId());

        order.setStatus(OrderStatus.CANCELLED);

        publishEvent(
            "order-events",
            OrderEvent.builder()
                .orderId(order.getOrderId())
                .eventType(OrderEvent.EventType.ORDER_CANCELLED)
                .build()
        );

        log.info(
            "[Saga 취소] 주문 {} - Saga 프로세스 취소됨",
            order.getOrderId()
        );
        sagaState.remove(order.getOrderId());
    }

    /**
     * Kafka로 이벤트 발행
     */
    private void publishEvent(String topic, OrderEvent event) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate
                .send(topic, event.getOrderId(), eventJson)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.debug(
                            "이벤트 발행 완료: {} → {}",
                            event.getEventType(),
                            topic
                        );
                    } else {
                        log.error("이벤트 발행 실패: {}", topic, ex);
                    }
                });
        } catch (Exception e) {
            log.error("이벤트 발행 실패", e);
        }
    }
}
