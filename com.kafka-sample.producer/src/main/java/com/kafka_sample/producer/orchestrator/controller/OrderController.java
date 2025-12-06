package com.kafka_sample.producer.orchestrator.controller;

import com.kafka_sample.producer.orchestrator.domain.Order;
import com.kafka_sample.producer.orchestrator.domain.OrderStatus;
import com.kafka_sample.producer.orchestrator.dto.OrderRequest;
import com.kafka_sample.producer.orchestrator.dto.OrderResponse;
import com.kafka_sample.producer.orchestrator.saga.OrderSagaOrchestrator;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 주문 API - Saga 패턴 시작점
 */
@RestController
@RequiredArgsConstructor
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderSagaOrchestrator sagaOrchestrator;

    /**
     * 주문 생성 - Saga 시작
     */
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@RequestBody OrderRequest request) {

        String orderId = "ORDER-" + UUID.randomUUID().toString().substring(0, 8);

        Order order = Order.builder()
            .orderId(orderId)
            .productId(request.getProductId())
            .quantity(request.getQuantity())
            .amount(request.getAmount())
            .customerId(request.getCustomerId())
            .status(OrderStatus.PENDING)
            .build();

        // Saga 시작
        sagaOrchestrator.startOrderSaga(order);

        return ResponseEntity.accepted().body(new OrderResponse(
            orderId,
            "Order created and Saga started. Check logs for processing status.",
            OrderStatus.PENDING
        ));
    }
}



