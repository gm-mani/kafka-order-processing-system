# Schema evolution

Subject: `orders-topic-value` | Compatibility: BACKWARD | Source: `order-events/src/main/avro/OrderCreatedEvent.avsc`

## Rules under BACKWARD

| Change | Allowed | Why |
| --- | --- | --- |
| Add a field with a default | Yes | The new reader fills the default when reading old data |
| Remove a field | Yes | The new reader ignores the extra field in old data |
| Add a field without a default | No | The new reader has no value for old data |
| Change a field's type incompatibly | No | Old data cannot be read as the new type |

Upgrade order: consumers first, then producers. A new consumer can read old and new events.

## Versions

| Version | Change |
| --- | --- |
| 1 | eventId, orderId, productName, price |
| 2 | Added `currency` (default `INR`) |

## Evidence

### v2 accepted

(paste the script output here)

### Breaking draft rejected

(paste the script output here)