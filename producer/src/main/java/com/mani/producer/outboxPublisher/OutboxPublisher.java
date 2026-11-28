package com.mani.producer.outboxPublisher;

import com.mani.producer.service.OutboxRelayService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxRelayService relayService;

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:1000}")
    public void publishEvents() {
        try {
            int published = relayService.publishBatch();
            if (published > 0) {
                log.info("Outbox relay published {} event(s)", published);
            }
        } catch (RuntimeException e) {
            log.error("Outbox relay run failed; will retry on next poll", e);
        }
    }
}