package com.mani.consumer.store;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class ProcessedOrdersStore {

    private final Set<String> processedOrders =
            ConcurrentHashMap.newKeySet();

    public boolean alreadyProcessed(String orderId) {
        boolean isProcessed = processedOrders.contains(orderId);
        log.debug("Checking if order {} is already processed: {}", orderId, isProcessed);
        return isProcessed;
    }

    public void markProcessed(String orderId) {
        processedOrders.add(orderId);
        log.debug("Marked order {} as processed. Total processed: {}", orderId, processedOrders.size());
    }
}