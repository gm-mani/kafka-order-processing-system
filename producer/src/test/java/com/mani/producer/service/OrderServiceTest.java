package com.mani.producer.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.producer.entity.Order;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.model.OrderRequest;
import com.mani.producer.repository.OrderRepository;
import com.mani.producer.repository.OutboxRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class OrderServiceTest {

    @Mock
    OrderRepository orderRepository;

    @Mock
    OutboxRepository outboxRepository;

    @Test
    public void createOrder_savesOneOrderAndOneOutboxRow_withMatchingEventId() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        OrderService orderService = new OrderService(orderRepository, outboxRepository, objectMapper);

        orderService.createOrder(new OrderRequest("o-1", "Laptop", new BigDecimal("999.99")));

        ArgumentCaptor<Order> orderCaptor = ArgumentCaptor.forClass(Order.class);
        verify(orderRepository).save(orderCaptor.capture());

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxRepository).save(outboxCaptor.capture());

        Order savedOrder = orderCaptor.getValue();
        OutboxEvent savedOutbox = outboxCaptor.getValue();

        assertThat(savedOrder.getOrderId()).isEqualTo("o-1");
        assertThat(savedOrder.getPrice()).isEqualByComparingTo("999.99");
        assertThat(savedOrder.getStatus()).isEqualTo("CREATED");

        assertThat(savedOutbox.getAggregateId()).isEqualTo("o-1");
        assertThat(savedOutbox.getEventType()).isEqualTo("ORDER_CREATED");
        assertThat(savedOutbox.getPublished()).isFalse();

        String eventIdInPayload = objectMapper
                .readTree(savedOutbox.getPayload())
                .get("eventId")
                .asText();
        assertThat(eventIdInPayload).isEqualTo(savedOutbox.getEventId());
        assertThat(objectMapper.readTree(savedOutbox.getPayload()).get("currency").asText()).isEqualTo("INR");

    }

    @Test
    void createOrder_throws_whenSerializationFails() throws Exception {
        ObjectMapper brokenMapper = mock(ObjectMapper.class);
        when(brokenMapper.writeValueAsString(any()))
                .thenThrow(new JsonProcessingException("boom") {
                });
        OrderService service = new OrderService(orderRepository, outboxRepository, brokenMapper);

        assertThatThrownBy(() -> service.createOrder(new OrderRequest("o-2", "Phone", new BigDecimal("999.99"))))
                .isInstanceOf(IllegalStateException.class)
                .hasCauseInstanceOf(JsonProcessingException.class);

        verify(outboxRepository, never()).save(any());
    }

}
