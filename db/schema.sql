-- order-experience-api — database schema
-- Reconstructed from the SQL embedded in the (previously commented-out) Mule flows:
--   orders/orders-api.xml, self-healing/09-outbox-relay.xml, orders/order-notification-consumer.xml
-- Run this once against a fresh `orderdb` before starting the app.

CREATE TABLE IF NOT EXISTS api_idempotency (
    idempotency_key     VARCHAR(255) PRIMARY KEY,
    external_order_id   VARCHAR(255) NOT NULL,
    internal_order_id   VARCHAR(255) NOT NULL,
    request_hash        VARCHAR(64)  NOT NULL,
    request_status      VARCHAR(20)  NOT NULL DEFAULT 'PROCESSING', -- PROCESSING | COMPLETED
    response_code       INTEGER,
    response_body       JSONB,
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id            VARCHAR(255) PRIMARY KEY,
    external_order_id   VARCHAR(255) NOT NULL UNIQUE,
    order_status        VARCHAR(20)  NOT NULL,          -- RECEIVED | ...
    order_payload       JSONB        NOT NULL,
    correlation_id      VARCHAR(255),
    created_at          TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS order_outbox (
    event_id             VARCHAR(255) PRIMARY KEY,
    event_type           VARCHAR(100) NOT NULL,
    event_version        VARCHAR(10)  NOT NULL,
    aggregate_type        VARCHAR(50)  NOT NULL,
    aggregate_id          VARCHAR(255) NOT NULL,
    event_payload         JSONB        NOT NULL,
    event_status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING', -- PENDING | PUBLISHED | FAILED
    retry_count            INTEGER      NOT NULL DEFAULT 0,
    last_error             TEXT,
    correlation_id          VARCHAR(255),
    created_at              TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at            TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_order_outbox_pending ON order_outbox (event_status, created_at) WHERE event_status = 'PENDING';

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_name       VARCHAR(255) NOT NULL,
    event_id            VARCHAR(255) NOT NULL,
    processed_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (consumer_name, event_id)
);
