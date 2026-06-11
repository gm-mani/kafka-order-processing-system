package com.mani.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.producer.entity.Order;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.model.OrderCreatedPayload;
import com.mani.producer.model.OrderRequest;
import com.mani.producer.repository.OrderRepository;
import com.mani.producer.repository.OutboxRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void createOrder(OrderRequest request) {

        Order order = new Order();
        order.setOrderId(request.getOrderId());
        order.setProductName(request.getProductName());
        order.setPrice(request.getPrice());

        orderRepository.save(order);
        log.debug("Order saved to database with ID: {}", order.getOrderId());

        OrderCreatedPayload orderCreatedPayload = OrderCreatedPayload.builder()
                .eventId(UUID.randomUUID().toString())
                .orderId(order.getOrderId())
                .productName(request.getProductName())
                .price(request.getPrice())
                .build();

        String orderPayload;

        try {
            orderPayload = objectMapper.writeValueAsString(orderCreatedPayload);
        } catch (JsonProcessingException e) {
            log.error("Error serializing OrderCreatedEvent: {}", e.getMessage());
            return;
        }

        OutboxEvent event = new OutboxEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setAggregateId(order.getOrderId());
        event.setEventType("ORDER_CREATED");
        event.setPayload(orderPayload);
        event.setPublished(false);
        event.setCreatedAt(LocalDateTime.now());

        outboxRepository.save(event);
        log.debug("Outbox event created for order: {} with event ID: {}", order.getOrderId(), event.getEventId());
    }
}
