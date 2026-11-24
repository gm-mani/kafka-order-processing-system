package com.mani.consumer.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Table(name = "processed_events")
@Getter
public class ProcessedOrder {

    @Id
    @Column(name = "event_id")
    private String eventId;

    @Column(name = "order_id")
    private String orderId;

    @Column(name = "processed_at")
    private LocalDateTime processedAt;

    protected ProcessedOrder() {
    }

    public ProcessedOrder(String eventId, String orderId,
                          LocalDateTime processedAt) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.processedAt = processedAt;
    }
}