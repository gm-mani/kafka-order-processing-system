package com.mani.consumer.model;

public record OrderCreatedEvent(String eventId,
                                String orderId,
                                String productName,
                                String price) {
}