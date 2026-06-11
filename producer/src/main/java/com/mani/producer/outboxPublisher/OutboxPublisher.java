package com.mani.producer.outboxPublisher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.kafka.avro.OrderCreatedEvent;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.model.OrderCreatedPayload;
import com.mani.producer.repository.OutboxRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@AllArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Scheduled(fixedDelay = 10000)
    public void publishEvents() {

        List<OutboxEvent> events = outboxRepository.findByPublishedFalse();

        if (events.isEmpty()) {
            log.debug("No unpublished events found");
            return;
        }

        log.info("Publishing {} unpublished event(s)", events.size());

        for (OutboxEvent outboxEvent : events) {
            try {
                OrderCreatedPayload orderPayload = objectMapper.readValue(
                        outboxEvent.getPayload(),
                        OrderCreatedPayload.class);

                OrderCreatedEvent event = OrderCreatedEvent.newBuilder()
                        .setEventId(orderPayload.eventId())
                        .setOrderId(orderPayload.orderId())
                        .setProductName(orderPayload.productName())
                        .setPrice(orderPayload.price())
                        .build();

                kafkaTemplate.send(
                        "orders-topic",
                        outboxEvent.getAggregateId(),
                        event
                ).get(5, TimeUnit.SECONDS);

                log.debug("Event published to Kafka - Event ID: {}, Order ID: {}",
                        outboxEvent.getEventId(), outboxEvent.getAggregateId());

                outboxEvent.setPublished(true);
                outboxRepository.save(outboxEvent);

            } catch (ExecutionException e) {
                log.error("Kafka send failed for event {}: {}",
                        outboxEvent.getEventId(), e.getCause().getMessage());
            } catch (TimeoutException e) {
                log.error("Kafka ack timed out for event {}", outboxEvent.getEventId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while publishing event {}", outboxEvent.getEventId());
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        log.info("Outbox publish run complete");
    }
}
