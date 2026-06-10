package com.mani.producer.OutboxPublisher;

import com.mani.producer.Entity.OutboxEvent;
import com.mani.producer.Repository.OutboxRepository;
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
    private final KafkaTemplate<String, String> kafkaTemplate;

    @Scheduled(fixedDelay = 5000)
    public void publishEvents() {

        List<OutboxEvent> events = outboxRepository.findByPublishedFalse();

        if (events.isEmpty()) {
            log.debug("No unpublished events found");
            return;
        }

        log.info("Publishing {} unpublished event(s)", events.size());

        for (OutboxEvent event : events) {
            try {
                kafkaTemplate.send(
                        "orders-topic",
                        event.getAggregateId(),
                        event.getPayload()
                ).get(5, TimeUnit.SECONDS); // block until broker acks or timeout

                log.debug("Event published to Kafka - Event ID: {}, Order ID: {}",
                        event.getEventId(), event.getAggregateId());

                event.setPublished(true);
                outboxRepository.save(event);

            } catch (ExecutionException e) {
                log.error("Failed to publish event {} to Kafka — will retry on next run: {}",
                        event.getEventId(), e.getCause().getMessage());
            } catch (TimeoutException e) {
                log.error("Timed out waiting for Kafka ack on event {} — will retry on next run",
                        event.getEventId());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted while publishing event {}", event.getEventId());
            }
        }

        log.info("Outbox publish run complete");
    }
}
