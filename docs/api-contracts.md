# API and Event Contracts

This document describes the main REST API endpoints and Kafka event contracts used in the Orion Distributed Order System.

---

# REST API

## Order Service

### Create Order

`POST /orders`

### Request

```json
{
  "product_name": "Laptop",
  "quantity": 2
}
```

### Response

```json
{
  "id": 1,
  "product_name": "Laptop",
  "quantity": 2,
  "status": "PENDING"
}
```

---

### Health Check

`GET /health`

### Response

```json
{
  "service": "order-service",
  "status": "ok"
}
```

---

# Kafka Event Contracts

All services communicate asynchronously through Kafka events.

Events follow a shared structure.

---

## Base Event Structure

```json
{
  "event_id": "uuid",
  "event_type": "OrderCreated",
  "version": 1,
  "occurred_at": "2026-05-12T12:00:00Z",
  "source": "order-service",
  "correlation_id": "uuid",
  "causation_id": "uuid",
  "payload": {}
}
```

---

## Event Metadata

| Field | Description |
|---|---|
| `event_id` | Unique event identifier |
| `event_type` | Type of the event |
| `version` | Event schema version |
| `occurred_at` | UTC timestamp of event creation |
| `source` | Service that produced the event |
| `correlation_id` | Shared workflow identifier |
| `causation_id` | Previous event that triggered the current event |
| `payload` | Business-specific event data |

---

# Order Events

## OrderCreated

Topic:

```text
order-events
```

Payload:

```json
{
  "order_id": 1,
  "product_name": "Laptop",
  "quantity": 2,
  "status": "PENDING"
}
```

---

## OrderCancelled

Topic:

```text
order-events
```

Payload:

```json
{
  "order_id": 1,
  "status": "CANCELLED"
}
```

---

# Inventory Events

## InventoryReserved

Topic:

```text
inventory-events
```

Payload:

```json
{
  "order_id": 1,
  "product_name": "Laptop",
  "quantity": 2,
  "status": "RESERVED"
}
```

---

## InventoryReleased

Topic:

```text
inventory-events
```

Payload:

```json
{
  "order_id": 1,
  "product_name": "Laptop",
  "quantity": 2,
  "status": "RELEASED"
}
```

---

# Payment Events

## PaymentCompleted

Topic:

```text
payment-events
```

Payload:

```json
{
  "order_id": 1,
  "status": "COMPLETED"
}
```

---

## PaymentFailed

Topic:

```text
payment-events
```

Payload:

```json
{
  "order_id": 1,
  "status": "FAILED"
}
```

---

# Shipping Events

## ShippingStarted

Topic:

```text
shipping-events
```

Payload:

```json
{
  "order_id": 1,
  "status": "SHIPPING_STARTED"
}
```

---

# Reliability Guarantees

The system includes several reliability patterns:

- Transactional Outbox Pattern
- Idempotent Consumers
- Retry Tracking
- Failed Event Recovery
- Correlation ID Propagation
- Timezone-aware timestamps
- Service-isolated Alembic migrations

---

# Notes

This project is intentionally focused on distributed backend architecture patterns rather than frontend functionality or UI design.