package com.mani.producer.scheduler;


import com.mani.producer.service.OutboxCleanupService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxCleanupScheduler {

    private final OutboxCleanupService outboxCleanupService;

    @Value("${outbox.cleanup.retention-days}")
    private int retentionDays;

    @Value("${outbox.cleanup.batch-size}")
    private int batchSize;

    @Scheduled(cron = "${outbox.cleanup.cron}")
    public void cleanup() {
        outboxCleanupService.cleanup(
                retentionDays,
                batchSize
        );
    }
}
