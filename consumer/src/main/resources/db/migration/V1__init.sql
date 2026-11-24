CREATE TABLE processed_events (
    event_id     VARCHAR(255) PRIMARY KEY,
    order_id     VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_events_order_id ON processed_events (order_id);