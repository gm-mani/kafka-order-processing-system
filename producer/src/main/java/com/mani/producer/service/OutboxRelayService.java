package com.mani.producer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mani.kafka.avro.OrderCreatedEvent;
import com.mani.producer.config.KafkaTopicConfig;
import com.mani.producer.entity.OutboxEvent;
import com.mani.producer.entity.OutboxStatus;
import com.mani.producer.model.OrderCreatedPayload;
import com.mani.producer.repository.OutboxRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.RetriableException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
public class OutboxRelayService {

    private static final long SEND_TIMEOUT_SECONDS = 15;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final int maxRetries;

    public OutboxRelayService(OutboxRepository outboxRepository,
                              KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate,
                              ObjectMapper objectMapper,
                              @Value("${outbox.relay.batch-size:20}") int batchSize,
                              @Value("${outbox.relay.max-retries:5}") int maxRetries) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.batchSize = batchSize;
        this.maxRetries = maxRetries;
    }

    @Transactional
    public int publishBatch() {
        List<OutboxEvent> rows = outboxRepository.lockNextBatch(batchSize);

        int published = 0;
        for (OutboxEvent row : rows) {
            boolean keepGoing = publishOne(row);
            if (!keepGoing) {
                break; // Kafka had a problem: stop here, the rest stay PENDING for the next run
            }
            if (row.getStatus() == OutboxStatus.PUBLISHED) {
                published++;
            }
        }
        return published;
    }

    // Returns true if the loop may continue with the next row, false if it should stop.
    private boolean publishOne(OutboxEvent row) {
        OrderCreatedEvent event;
        try {
            event = toAvro(row);
        } catch (Exception e) {
            row.setStatus(OutboxStatus.FAILED);
            row.setLastError("Unreadable payload: " + e.getMessage());
            log.error("Outbox event {} parked as FAILED: {}", row.getEventId(), row.getLastError());
            return true; // this row is hopeless, but the next one may be fine
        }

        try {
            kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, row.getAggregateId(), event)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            row.setStatus(OutboxStatus.PUBLISHED);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            recordFailure(row, e);
            return false;
        }
    }

    private void recordFailure(OutboxEvent row, Exception error) {
        row.setLastError(error.getClass().getSimpleName() + ": " + error.getMessage());

        if (isKafkaOutage(error)) {
            log.warn("Kafka unavailable, event {} stays PENDING and will be retried", row.getEventId());
            return; // an outage is not the row's fault, so it is not counted
        }

        row.setRetryCount(row.getRetryCount() + 1);
        if (row.getRetryCount() >= maxRetries) {
            row.setStatus(OutboxStatus.FAILED);
            log.error("Outbox event {} parked as FAILED after {} attempts: {}",
                    row.getEventId(), row.getRetryCount(), row.getLastError());
        } else {
            log.warn("Outbox event {} failed (attempt {}/{}): {}",
                    row.getEventId(), row.getRetryCount(), maxRetries, row.getLastError());
        }
    }

    private boolean isKafkaOutage(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof RetriableException || t instanceof TimeoutException || t instanceof IOException) {
                return true;
            }
        }
        return false;
    }

    private OrderCreatedEvent toAvro(OutboxEvent row) throws JsonProcessingException {
        OrderCreatedPayload payload = objectMapper.readValue(row.getPayload(), OrderCreatedPayload.class);
        return OrderCreatedEvent.newBuilder()
                .setEventId(payload.eventId())
                .setOrderId(payload.orderId())
                .setProductName(payload.productName())
                .setPrice(payload.price())
                .setCurrency(payload.currency() != null ? payload.currency() : "INR")
                .build();
    }
}