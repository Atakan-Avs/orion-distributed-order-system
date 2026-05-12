# Architecture Overview

Orion Distributed Order System is a production-oriented microservices project that demonstrates event-driven architecture, distributed transaction handling, and reliability patterns commonly used in backend systems.

The system models an order lifecycle across multiple independent services:

- Order Service
- Inventory Service
- Payment Service
- Shipping Service
- Notification Service

Services communicate asynchronously through Kafka events. Each service owns its own responsibility and persists its own processing state.

---

## High-Level Flow

```text
Client
  |
  v
Order Service
  |
  | OrderCreated
  v
Kafka
  |
  v
Inventory Service
  |
  | InventoryReserved
  v
Kafka
  |
  v
Payment Service
  |
  | PaymentCompleted / PaymentFailed
  v
Kafka
  |
  v
Shipping Service
  |
  | ShippingStarted / ShippingFailed
  v
Kafka
  |
  v
Notification Service
```

---

## Saga Pattern

The project uses a choreography-based Saga approach.

Each service reacts to domain events and publishes the next event in the workflow.

### Example Successful Flow

```text
OrderCreated
  -> InventoryReserved
  -> PaymentCompleted
  -> ShippingStarted
  -> NotificationSent
```

### Example Failure / Compensation Flow

```text
OrderCreated
  -> InventoryReserved
  -> PaymentFailed
  -> InventoryReleased
  -> OrderCancelled
```

This avoids a single distributed database transaction and keeps services loosely coupled.

---

## Transactional Outbox Pattern

Services that publish events use the Transactional Outbox pattern.

Instead of publishing directly to Kafka during business logic execution, the service writes the outgoing event to an outbox table inside the same database transaction.

A background publisher reads pending outbox records and publishes them to Kafka.

This improves reliability because the database write and event creation happen atomically.

---

## Idempotent Consumers

Consumers store processed event IDs to prevent duplicate event processing.

This is important because Kafka consumers may receive the same event more than once in real-world distributed systems.

Each consumer checks whether an event was already processed before executing business logic.

---

## Correlation and Causation IDs

Events include:

- `event_id`
- `correlation_id`
- `causation_id`

These fields make it easier to trace a distributed workflow across multiple services.

- `event_id`: unique ID of the current event
- `correlation_id`: shared ID across the full business flow
- `causation_id`: ID of the event that caused the current event

---

## Database Migrations

Each service uses Alembic for schema migration management.

Every service owns its own migration lifecycle and uses an isolated Alembic version table:

- `alembic_version` for Order Service
- `payment_alembic_version`
- `inventory_alembic_version`
- `shipping_alembic_version`
- `notification_alembic_version`

This prevents migration conflicts when multiple services share the same development database.

---

## Reliability Patterns

The project includes several production-style backend patterns:

- Event-driven communication
- Saga-based workflow
- Transactional outbox
- Idempotent consumers
- Retry tracking
- Failed event handling
- Dead-letter style recovery logic
- Correlation ID propagation
- Metrics endpoint
- Alembic migration lifecycle

---

## Current Scope

This project is designed as a portfolio-grade backend architecture project.

It is not intended to be a full e-commerce application. Instead, it focuses on distributed backend system design and reliability patterns.