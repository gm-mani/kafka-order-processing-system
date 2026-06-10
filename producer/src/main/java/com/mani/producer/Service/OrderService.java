package com.mani.producer.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.producer.Entity.Order;
import com.mani.producer.Entity.OutboxEvent;
import com.mani.producer.Repository.OrderRepository;
import com.mani.producer.Repository.OutboxRepository;
import com.mani.producer.model.OrderCreatedEvent;
import com.mani.producer.model.OrderRequest;
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

        OrderCreatedEvent orderCreatedEvent = new OrderCreatedEvent();
        orderCreatedEvent.setOrderId(order.getOrderId());
        orderCreatedEvent.setProductName(request.getProductName());
        orderCreatedEvent.setPrice(request.getPrice());
        orderCreatedEvent.setEventId(UUID.randomUUID().toString());

        OutboxEvent event = new OutboxEvent();
        event.setEventId(UUID.randomUUID().toString());
        event.setAggregateId(order.getOrderId());
        event.setEventType("ORDER_CREATED");

        try {
            event.setPayload(objectMapper.writeValueAsString(orderCreatedEvent));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(
                    "Failed to serialize OrderCreatedEvent for order: " + order.getOrderId(), e
            );
        }

        event.setPublished(false);
        event.setCreatedAt(LocalDateTime.now());

        outboxRepository.save(event);
        log.debug("Outbox event created for order: {} with event ID: {}", order.getOrderId(), event.getEventId());
    }
}
