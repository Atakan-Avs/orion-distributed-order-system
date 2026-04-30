# ORION Saga Flow

ORION uses an event-driven Saga pattern to coordinate the distributed order lifecycle across multiple services.

## Successful Order Flow

```text
Client
  ↓
Order Service
  ↓ OrderCreated
Kafka: order-events
  ↓
Inventory Service
  ↓ InventoryReserved
Kafka: inventory-events
  ↓
Payment Service
  ↓ PaymentCompleted
Kafka: payment-events
  ↓
Shipping Service
  ↓ ShippingCreated
Kafka: shipping-events
  ↓
Notification Service
  ↓ NotificationSent
Event Chain
Step	Service	Consumes	Produces
1	order-service	HTTP POST /orders	OrderCreated
2	inventory-service	OrderCreated	InventoryReserved
3	payment-service	InventoryReserved	PaymentCompleted
4	shipping-service	PaymentCompleted	ShippingCreated
5	notification-service	ShippingCreated	NotificationSent
Current Status

The current implementation demonstrates the happy path of a distributed Saga flow.

Future improvements:

Compensation flow
Payment failure handling
Inventory release
Transactional outbox
Idempotency keys
Retry and dead-letter queue