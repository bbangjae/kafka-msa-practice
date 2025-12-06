package com.kafka_sample.producer.orchestrator.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OrderRequest {

    private String productId;
    private Integer quantity;
    private Double amount;
    private String customerId;
}
