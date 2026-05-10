# ORION Saga Flow

ORION uses an event-driven Saga pattern to coordinate the distributed order lifecycle across multiple services.

The system is designed around asynchronous Kafka events, eventual consistency, compensation flows, idempotent consumers, and reliable event publishing with the Transactional Outbox Pattern.

---

# Core Services

```text
order-service
inventory-service
payment-service
shipping-service
notification-service
```

---

# Successful Order Flow

```text
Client
↓
Order Service
↓
OrderCreated written to order outbox
↓
Order Outbox Publisher
↓
Kafka: order-events
↓
Inventory Service
↓
InventoryReserved written to inventory outbox
↓
Inventory Outbox Publisher
↓
Kafka: inventory-events
↓
Payment Service
↓
PaymentCompleted written to payment outbox
↓
Payment Outbox Publisher
↓
Kafka: payment-events
↓
Shipping Service
↓
ShippingCreated
↓
Kafka: shipping-events
↓
Notification Service
↓
NotificationSent
```

---

# Compensation Flow: Payment Failure

```text
Client
↓
Order Service
↓
OrderCreated written to order outbox
↓
Kafka: order-events
↓
Inventory Service
↓
InventoryReserved written to inventory outbox
↓
Kafka: inventory-events
↓
Payment Service
↓
PaymentFailed written to payment outbox
↓
Kafka: payment-events
↓
Inventory Service
↓
InventoryReleased written to inventory outbox
↓
Kafka: inventory-events
↓
Order Service
↓
OrderCancelled written to order outbox
↓
Kafka: order-events
↓
Notification Service
↓
CancellationNotificationSent
```

---

# Event Chain

| Step | Service | Consumes | Produces |
|---|---|---|---|
| 1 | order-service | HTTP POST /orders | OrderCreated |
| 2 | inventory-service | OrderCreated | InventoryReserved |
| 3 | payment-service | InventoryReserved | PaymentCompleted or PaymentFailed |
| 4A | shipping-service | PaymentCompleted | ShippingCreated |
| 4B | inventory-service | PaymentFailed | InventoryReleased |
| 5B | order-service | InventoryReleased | OrderCancelled |
| 6 | notification-service | ShippingCreated or OrderCancelled | NotificationSent |

---

# Reliability Layer

Transactional Outbox is implemented for the core Saga event publishers:

```text
order-service
inventory-service
payment-service
```

Outbox flow:

```text
Business operation
+
Outbox event insert
↓
Single database commit
↓
Background outbox publisher
↓
Kafka publish
↓
processed=true
```

---

# Failure Handling

If Kafka is unavailable:

```text
Event stays in outbox
↓
Publisher retries automatically
↓
retry_count increases
↓
After max retry limit, event becomes failed=true
```

Failed events can be recovered manually.

```text
POST /outbox/{event_id}/retry
```

---

# Idempotency

Consumers persist processed event IDs in PostgreSQL.

This prevents duplicate business processing when Kafka events are replayed or consumed more than once.

---

# Correlation and Causation IDs

Each event contains:

```text
correlation_id
causation_id
```

These fields make the event chain traceable across services.

---

# Current Status

Implemented:

```text
Saga Pattern
Compensation Flow
Transactional Outbox
Retry Mechanism
Failed Event State
Manual Recovery
Idempotent Consumers
Correlation/Causation IDs
```

Next improvements:

```text
Dead Letter Queue
Inbox Pattern
Prometheus Metrics
Distributed Tracing
Kafka Manual Offset Management
Saga State Persistence
```