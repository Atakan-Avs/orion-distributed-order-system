# Orion Distributed Order System

Production-oriented distributed order processing system built with Python, FastAPI, Kafka, PostgreSQL, Docker, and Saga architecture.

This project focuses on real-world distributed systems concepts such as asynchronous communication, reliable event publishing, compensation flows, idempotent consumers, and failure recovery.

---

# Architecture Overview

```text
Order Service
    ↓
Kafka: OrderCreated
    ↓
Inventory Service
    ↓
Kafka: InventoryReserved
    ↓
Payment Service
    ↓
Kafka: PaymentCompleted OR PaymentFailed
    ↓
Shipping Service
    ↓
Kafka: ShippingCreated
    ↓
Order COMPLETED
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

* Python
* FastAPI
* SQLAlchemy
* PostgreSQL

## Messaging

* Apache Kafka
* Event-driven architecture
* Saga orchestration flow

## Infrastructure

* Docker
* Docker Compose

## Reliability Patterns

* Transactional Outbox Pattern
* Idempotent Consumer Pattern
* Retry Mechanism
* Failed Event Recovery
* Compensation Transactions

---

# Services

## Order Service

Responsibilities:

* Create orders
* Publish OrderCreated events
* Track saga state transitions
* Handle compensation events
* Mark orders as COMPLETED or CANCELLED

Key Features:

* Transactional Outbox
* Reliable event publishing
* Compensation handling
* Saga state tracking
* Correlation/Causation ID support

---

## Inventory Service

Responsibilities:

* Reserve inventory
* Release inventory on payment failure
* Publish inventory events

Key Features:

* Transactional Outbox
* Idempotent event consumption
* Compensation flow support
* Retry & failed event handling

---

## Payment Service

Responsibilities:

* Process payments
* Publish PaymentCompleted or PaymentFailed events

Key Features:

* Transactional Outbox
* Idempotent consumer
* Failure simulation support
* Retry & recovery handling

---

## Shipping Service

Responsibilities:

* Create shipment records
* Publish ShippingCreated events
* Complete successful saga flows

Key Features:

* Transactional Outbox
* Idempotent consumer support
* Final saga completion handling

---

## Notification Service

Responsibilities:

* Consume order lifecycle events
* Simulate asynchronous notifications

Key Features:

* Event-driven notification flow
* Kafka consumer architecture

---

# Distributed Systems Features

## Saga Pattern

The system uses asynchronous Saga orchestration between services.

Successful Flow:

```text
OrderCreated
→ InventoryReserved
→ PaymentCompleted
→ ShippingCreated
→ Order COMPLETED
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

* order-service
* inventory-service
* payment-service
* shipping-service

Instead of publishing directly to Kafka:

```text
Business Event
→ Database Outbox Table
→ Background Publisher
→ Kafka
```

Benefits:

* Prevents lost events
* Reliable async publishing
* Eventual consistency support
* Recovery after Kafka outages

---

## Retry Mechanism

Outbox publishers automatically retry failed Kafka publishes.

Features:

* Retry count tracking
* Failed state management
* Last error persistence
* Recovery after Kafka restart

---

## Failed Event Recovery

The system supports manual recovery for failed outbox events.

Features:

* Failed event listing endpoint
* Manual retry endpoint
* Persistent failure tracking

Endpoints:

```text
GET  /admin/outbox/failed
POST /admin/outbox/{event_id}/retry
```

---

## Idempotent Consumers

Consumers store processed event IDs to prevent duplicate event processing.

Benefits:

* Prevents duplicate business operations
* Safe Kafka reprocessing
* Reliable distributed event handling

---

## Correlation & Causation IDs

Every event contains:

* correlation_id
* causation_id

This enables:

* Distributed tracing
* Event chain tracking
* Debugging across services

---

# System Guarantees

The system is designed around eventual consistency principles.

Key guarantees:

* Reliable event publishing with Transactional Outbox
* Duplicate event protection with Idempotent Consumers
* Compensation-based failure recovery
* Persistent failed event tracking
* Manual operational recovery support

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
```

---

# Running the Project

## 1. Start Infrastructure

```bash
docker compose up --build
```

---

## 2. Start Services

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
docker compose up
```

Then run:

```bash
pytest tests/test_order_saga_integration.py
```

---

# Failure Simulation

The payment service intentionally fails payments when:

```text
quantity > 5
```

This allows testing:

* Saga compensation
* Inventory release flow
* Order cancellation flow
* Outbox retry & recovery

---

# Reliability Testing Performed

Tested scenarios:

* Kafka outage simulation
* Automatic retry handling
* Failed event persistence
* Manual event recovery
* Compensation flow execution
* Idempotent consumer validation
* Event replay safety
* Saga state transition validation

---

# Future Improvements

Potential next steps:

* Inbox Pattern
* Distributed tracing with OpenTelemetry
* Prometheus metrics
* Grafana dashboards
* Kafka manual offset management
* Kubernetes deployment
* CI/CD pipeline

---

# Learning Goals

This project was built to deeply understand:

* Distributed systems
* Event-driven architecture
* Saga pattern
* Reliable asynchronous communication
* Failure recovery strategies
* Production-oriented backend design

---

# Author

Atakan Avsever

GitHub:
https://github.com/Atakan-Avs

LinkedIn:
https://linkedin.com/in/atakanavsever