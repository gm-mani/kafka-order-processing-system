package com.mani.consumer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_orders")
@Getter
public class ProcessedOrder {

    @Id
    @Column(name = "order_id")
    private String orderId;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    protected ProcessedOrder() {
    }

    public ProcessedOrder(String orderId,
                          LocalDateTime processedAt) {
        this.orderId = orderId;
        this.processedAt = processedAt;
    }
}