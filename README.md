# Orion Distributed Order System

Production-oriented distributed order processing system built with Python, FastAPI, Kafka, PostgreSQL, Docker, and Saga architecture.

This project focuses on real-world distributed systems concepts such as asynchronous communication, reliable event publishing, compensation flows, idempotent consumers, structured logging, distributed tracing support, schema migration management, and failure recovery.

---

# Architecture Overview

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

Compensation Flow:

```text
OrderCreated
→ InventoryReserved
→ PaymentFailed
→ InventoryReleased
→ OrderCancelled
```

---

# Tech Stack

## Backend

- Python
- FastAPI
- SQLAlchemy
- PostgreSQL
- Alembic

## Messaging

- Apache Kafka
- Event-driven architecture
- Saga choreography flow

## Infrastructure

- Docker
- Docker Compose

## Reliability Patterns

- Transactional Outbox Pattern
- Idempotent Consumer Pattern
- Retry Mechanism
- Failed Event Recovery
- Compensation Transactions

---

# Services

## Order Service

Responsibilities:

- Create orders
- Publish OrderCreated events
- Track saga state transitions
- Handle compensation events
- Mark orders as COMPLETED or CANCELLED

Key Features:

- Transactional Outbox
- Reliable event publishing
- Compensation handling
- Saga state tracking
- Correlation/Causation ID support
- Structured JSON logging
- Metrics endpoint
- Health check endpoint
- Alembic migrations

---

## Inventory Service

Responsibilities:

- Reserve inventory
- Release inventory on payment failure
- Publish inventory events

Key Features:

- Transactional Outbox
- Idempotent event consumption
- Compensation flow support
- Retry & failed event handling
- Alembic migrations

---

## Payment Service

Responsibilities:

- Process payments
- Publish PaymentCompleted or PaymentFailed events

Key Features:

- Transactional Outbox
- Idempotent consumer
- Failure simulation support
- Retry & recovery handling
- Alembic migrations

---

## Shipping Service

Responsibilities:

- Create shipment records
- Publish ShippingStarted events
- Complete successful saga flows

Key Features:

- Transactional Outbox
- Idempotent consumer support
- Final saga completion handling
- Alembic migrations

---

## Notification Service

Responsibilities:

- Consume order lifecycle events
- Simulate asynchronous notifications

Key Features:

- Event-driven notification flow
- Kafka consumer architecture
- Alembic migrations

---

# Distributed Systems Features

## Saga Pattern

The system uses asynchronous Saga choreography between services.

Successful Flow:

```text
OrderCreated
→ InventoryReserved
→ PaymentCompleted
→ ShippingStarted
→ NotificationSent
```

Compensation Flow:

```text
OrderCreated
→ InventoryReserved
→ PaymentFailed
→ InventoryReleased
→ OrderCancelled
```

---

## Saga State Tracking

Orders move through explicit distributed saga states:

```text
PENDING
→ INVENTORY_RESERVED
→ PAYMENT_COMPLETED
→ COMPLETED
```

Failure flow:

```text
PENDING / INVENTORY_RESERVED
→ CANCELLED
```

This improves observability and operational debugging of distributed workflows.

---

## Transactional Outbox Pattern

Implemented in:

- order-service
- inventory-service
- payment-service
- shipping-service

Instead of publishing directly to Kafka:

```text
Business Event
→ Database Outbox Table
→ Background Publisher
→ Kafka
```

Benefits:

- Prevents lost events
- Reliable async publishing
- Eventual consistency support
- Recovery after Kafka outages

---

## Retry Mechanism

Outbox publishers automatically retry failed Kafka publishes.

Features:

- Retry count tracking
- Failed state management
- Last error persistence
- Recovery after Kafka restart

---

## Failed Event Recovery

The system supports manual recovery for failed outbox events.

Features:

- Failed event listing endpoint
- Manual retry endpoint
- Persistent failure tracking

Endpoints:

```text
GET  /admin/outbox/failed
POST /admin/outbox/{event_id}/retry
```

---

## Metrics / Observability

Order Service exposes Prometheus-style metrics and operational endpoints.

Metrics endpoint:

```text
GET /metrics
```

Health check endpoint:

```text
GET /health
```

Example metrics output:

```text
orion_orders_created_total 10
orion_saga_completed_total 7
orion_saga_cancelled_total 2
orion_outbox_pending_total 1
orion_outbox_failed_total 0
```

These metrics help monitor:

- Total created orders
- Completed saga flows
- Cancelled saga flows
- Pending outbox events
- Failed outbox events

---

## Idempotent Consumers

Consumers store processed event IDs to prevent duplicate event processing.

Benefits:

- Prevents duplicate business operations
- Safe Kafka reprocessing
- Reliable distributed event handling

---

## Correlation & Causation IDs

Every event contains:

- correlation_id
- causation_id

This enables:

- Distributed tracing
- Event chain tracking
- Debugging across services

---

## Structured Logging

Order Service uses structured JSON logs for event processing.

Each important saga transition log includes:

- service name
- message
- event_type
- correlation_id
- causation_id
- order_id
- status

Example log:

```json
{
  "timestamp": "2026-05-11T12:00:00+00:00",
  "service": "order-service",
  "message": "Order marked as payment completed",
  "event_type": "PaymentCompleted",
  "correlation_id": "test-correlation-id-123",
  "causation_id": "payment-event-001",
  "order_id": 1,
  "status": "PAYMENT_COMPLETED"
}
```

This makes distributed saga flows easier to debug across services.

---

## Request Correlation ID

Order Service supports request-level correlation IDs.

Clients can pass:

```text
X-Correlation-ID: custom-correlation-id
```

If no correlation ID is provided, the service generates one automatically.

The correlation ID is propagated into the `OrderCreated` event and stored in the outbox payload, allowing the full distributed event chain to be traced from the initial HTTP request.

---

## Database Migration Lifecycle

Each service manages its own schema migrations using Alembic.

Service-specific migration version tables:

- alembic_version
- payment_alembic_version
- inventory_alembic_version
- shipping_alembic_version
- notification_alembic_version

Benefits:

- Independent schema evolution
- Safer service ownership
- Migration isolation
- Production-style database lifecycle management

---

# System Guarantees

The system is designed around eventual consistency principles.

Key guarantees:

- Reliable event publishing with Transactional Outbox
- Duplicate event protection with Idempotent Consumers
- Compensation-based failure recovery
- Persistent failed event tracking
- Manual operational recovery support
- Distributed request tracing support

---

# Project Structure

```text
orion-distributed-order-system/
│
├── infra/
│   └── docker-compose.yml
│
├── services/
│   ├── order-service/
│   ├── inventory-service/
│   ├── payment-service/
│   ├── shipping-service/
│   └── notification-service/
│
├── tests/
│
└── docs/
    ├── architecture.md
    └── api-contracts.md
```

---

# Running the Project

## 1. Start Infrastructure

```bash
docker compose -f infra/docker-compose.yml up -d
```

---

## 2. Run Database Migrations

### Order Service

```bash
cd services/order-service
alembic upgrade head
```

### Inventory Service

```bash
cd services/inventory-service
alembic upgrade head
```

### Payment Service

```bash
cd services/payment-service
alembic upgrade head
```

### Shipping Service

```bash
cd services/shipping-service
alembic upgrade head
```

### Notification Service

```bash
cd services/notification-service
alembic upgrade head
```

---

## 3. Start Services

### Order Service

```bash
cd services/order-service
uvicorn app.main:app --reload
```

### Inventory Service

```bash
cd services/inventory-service
python -m app.main
```

### Payment Service

```bash
cd services/payment-service
python -m app.main
```

### Shipping Service

```bash
cd services/shipping-service
python -m app.main
```

### Notification Service

```bash
cd services/notification-service
python -m app.main
```

---

# Swagger API

```text
http://127.0.0.1:8000/docs
```

---

# Integration Testing

Integration tests require running infrastructure and services.

Start containers first:

```bash
docker compose -f infra/docker-compose.yml up -d
```

Then run:

```bash
pytest
```

---

# Failure Simulation

The payment service intentionally fails payments when:

```text
quantity > 5
```

This allows testing:

- Saga compensation
- Inventory release flow
- Order cancellation flow
- Outbox retry & recovery

---

# Reliability Testing Performed

Tested scenarios:

- Kafka outage simulation
- Automatic retry handling
- Failed event persistence
- Manual event recovery
- Compensation flow execution
- Idempotent consumer validation
- Event replay safety
- Saga state transition validation
- Correlation ID propagation validation
- Metrics endpoint validation
- Alembic migration validation

---

# Future Improvements

Potential next steps:

- Inbox Pattern
- OpenTelemetry distributed tracing
- Prometheus integration
- Grafana dashboards
- Kafka manual offset management
- Kubernetes deployment
- GitHub Actions CI/CD pipeline
- Centralized logging stack

---

# Learning Goals

This project was built to deeply understand:

- Distributed systems
- Event-driven architecture
- Saga pattern
- Reliable asynchronous communication
- Failure recovery strategies
- Production-oriented backend design
- Distributed tracing concepts
- Operational observability
- Database migration lifecycle management

---

# Author

Atakan Avsever

GitHub:
https://github.com/Atakan-Avs

LinkedIn:
https://linkedin.com/in/atakanavsever