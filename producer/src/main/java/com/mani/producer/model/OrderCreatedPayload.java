package com.mani.producer.model;

import lombok.Builder;

@Builder
public record OrderCreatedPayload(
        String eventId,
        String orderId,
        String productName,
        String price
) {
}