package com.mani.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.producer.entity.Order;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.model.OrderCreatedPayload;
import com.mani.producer.model.OrderRequest;
import com.mani.producer.repository.OrderRepository;
import com.mani.producer.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
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

        BigDecimal price = request.getPrice().setScale(2);

        Order order = new Order();
        order.setOrderId(request.getOrderId());
        order.setProductName(request.getProductName());
        order.setPrice(price);
        order.setStatus("CREATED");
        order.setCreatedAt(LocalDateTime.now());

        orderRepository.save(order);
        log.debug("Order saved to database with ID: {}", order.getOrderId());

        String eventId = UUID.randomUUID().toString();
        OrderCreatedPayload orderCreatedPayload = OrderCreatedPayload.builder()
                .eventId(eventId)
                .orderId(order.getOrderId())
                .productName(request.getProductName())
                .price(price.toPlainString())
                .build();

        String orderPayload;

        try {
            orderPayload = objectMapper.writeValueAsString(orderCreatedPayload);
        } catch (JsonProcessingException e) {
            log.error("Error serializing OrderCreatedEvent: {}", e.getMessage());
            throw new IllegalStateException("Could not serialize order event for " + order.getOrderId(), e);
        }

        OutboxEvent event = new OutboxEvent();
        event.setEventId(eventId);
        event.setAggregateId(order.getOrderId());
        event.setEventType("ORDER_CREATED");
        event.setPayload(orderPayload);
        event.setPublished(false);
        event.setCreatedAt(LocalDateTime.now());

        outboxRepository.save(event);
        log.debug("Outbox event created for order: {} with event ID: {}", order.getOrderId(), event.getEventId());
    }
}
