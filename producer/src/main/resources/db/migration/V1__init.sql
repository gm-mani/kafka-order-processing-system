CREATE TABLE orders
(
    order_id     VARCHAR(255) PRIMARY KEY,
    product_name VARCHAR(255)   NOT NULL,
    price        NUMERIC(12, 2) NOT NULL,
    status       VARCHAR(32)    NOT NULL DEFAULT 'CREATED',
    created_at   TIMESTAMP      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE outbox_events
(
    id           BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(255) NOT NULL UNIQUE,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      TEXT         NOT NULL,
    published    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMP    NOT NULL
);

CREATE INDEX idx_outbox_published_id ON outbox_events (published, id);
-- The cleanup job filters on created_at.
CREATE INDEX idx_outbox_created_at ON outbox_events (created_at);