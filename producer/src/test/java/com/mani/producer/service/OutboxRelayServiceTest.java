package com.mani.producer.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.kafka.avro.OrderCreatedEvent;
import com.mani.producer.config.KafkaTopicConfig;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.entity.OutboxStatus;
import com.mani.producer.model.OrderCreatedPayload;
import com.mani.producer.repository.OutboxRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxRelayServiceTest {

    @Mock
    OutboxRepository outboxRepository;

    @Mock
    KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private OutboxRelayService relay;

    @BeforeEach
    void setUp() {
        // batch size 20, max retries 3
        relay = new OutboxRelayService(outboxRepository, kafkaTemplate, objectMapper, 20, 3);
    }

    private OutboxEvent row(long id, String orderId) throws Exception {
        String eventId = "evt-" + id;
        OutboxEvent e = new OutboxEvent();
        e.setId(id);
        e.setEventId(eventId);
        e.setAggregateId(orderId);
        e.setEventType("ORDER_CREATED");
        e.setPayload(objectMapper.writeValueAsString(OrderCreatedPayload.builder()
                .eventId(eventId).orderId(orderId).productName("Laptop")
                .price("999.99").currency("INR").build()));
        e.setStatus(OutboxStatus.PENDING);
        e.setCreatedAt(LocalDateTime.now());
        return e;
    }

    private void sendSucceeds(String orderId) {
        when(kafkaTemplate.send(eq(KafkaTopicConfig.TOPIC_NAME), eq(orderId), any(OrderCreatedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void sendFails(String orderId, Throwable error) {
        when(kafkaTemplate.send(eq(KafkaTopicConfig.TOPIC_NAME), eq(orderId), any(OrderCreatedEvent.class)))
                .thenReturn(CompletableFuture.failedFuture(error));
    }

    @Test
    void publishesRowAndMarksItPublished() throws Exception {
        OutboxEvent e = row(1, "o-1");
        when(outboxRepository.lockNextBatch(20)).thenReturn(List.of(e));
        sendSucceeds("o-1");

        int published = relay.publishBatch();

        assertThat(published).isEqualTo(1);
        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
    }

    @Test
    void stopsAtFirstFailure_andParksTheRowAfterMaxRetries() throws Exception {
        OutboxEvent bad = row(1, "o-1");
        OutboxEvent good = row(2, "o-2");
        when(outboxRepository.lockNextBatch(20))
                .thenReturn(List.of(bad, good))
                .thenReturn(List.of(bad, good))
                .thenReturn(List.of(bad, good))
                .thenReturn(List.of(good));
        sendFails("o-1", new IllegalStateException("bad record"));
        sendSucceeds("o-2");

        relay.publishBatch();
        assertThat(bad.getRetryCount()).isEqualTo(1);
        assertThat(good.getStatus()).isEqualTo(OutboxStatus.PENDING); // not even tried: we stopped at the failure

        relay.publishBatch();
        relay.publishBatch();
        assertThat(bad.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(bad.getRetryCount()).isEqualTo(3);
        assertThat(bad.getLastError()).contains("bad record");
        assertThat(good.getStatus()).isEqualTo(OutboxStatus.PENDING);

        relay.publishBatch(); // the parked row is no longer in the pending list
        assertThat(good.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
    }

    @Test
    void kafkaOutageNeverCountsAgainstTheRow() throws Exception {
        OutboxEvent e = row(1, "o-1");
        when(outboxRepository.lockNextBatch(20)).thenReturn(List.of(e));
        sendFails("o-1", new org.apache.kafka.common.errors.TimeoutException("expired"));

        for (int i = 0; i < 10; i++) {
            relay.publishBatch();
        }

        assertThat(e.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(e.getRetryCount()).isZero();
        assertThat(e.getLastError()).contains("TimeoutException");
    }

    @Test
    void unreadablePayloadIsParkedImmediately_andTheNextRowStillGoesOut() throws Exception {
        OutboxEvent broken = row(1, "o-1");
        broken.setPayload("{not json");
        OutboxEvent good = row(2, "o-2");
        when(outboxRepository.lockNextBatch(20)).thenReturn(List.of(broken, good));
        sendSucceeds("o-2");

        relay.publishBatch();

        assertThat(broken.getStatus()).isEqualTo(OutboxStatus.FAILED);
        assertThat(broken.getLastError()).contains("Unreadable payload");
        assertThat(good.getStatus()).isEqualTo(OutboxStatus.PUBLISHED);
        verify(kafkaTemplate, never()).send(eq(KafkaTopicConfig.TOPIC_NAME), eq("o-1"), any(OrderCreatedEvent.class));
    }
}