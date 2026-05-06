# ORION - Distributed Order System 🚀

ORION is a production-oriented distributed order management system that demonstrates a real-world microservices architecture using the Saga pattern and event-driven communication.

This project showcases how scalable backend systems are designed in production environments.

---

## 🧠 Architecture Overview

- Fully event-driven architecture (no direct service-to-service HTTP calls)
- Asynchronous communication via Kafka
- Saga pattern for distributed transaction management
- Each service is isolated and independently executable
- Designed for scalability, resilience and loose coupling

---

## ⚙️ Tech Stack

- FastAPI
- PostgreSQL
- Kafka
- Redis
- Docker Compose
- SQLAlchemy
- Python
- Pytest

---

## 🧩 Services

- **order-service** → creates orders and starts the Saga
- **inventory-service** → reserves stock and releases inventory on failure
- **payment-service** → processes payment and can fail
- **shipping-service** → creates shipment
- **notification-service** → sends success and cancellation notifications

---

## 🔁 Saga Flow

### ✅ Successful Flow

```text
OrderCreated
→ InventoryReserved
→ PaymentCompleted
→ ShippingCreated
→ NotificationSent
```

---

### ❌ Compensation Flow (Payment Failure)

```text
OrderCreated
→ InventoryReserved
→ PaymentFailed
→ InventoryReleased
→ OrderCancelled
→ CancellationNotificationSent
```

---

Detailed flow: `docs/saga-flow.md`

---

## ▶️ How to Run

### 1. Start infrastructure

```bash
cd infra
docker compose up -d
```

---

### 2. Start services (each in separate terminal)

#### Order Service

```bash
cd services/order-service
uvicorn app.main:app --reload
```

#### Inventory Service

```bash
cd services/inventory-service
python -m app.main
```

#### Payment Service

```bash
cd services/payment-service
python -m app.main
```

#### Shipping Service

```bash
cd services/shipping-service
python -m app.main
```

#### Notification Service

```bash
cd services/notification-service
python -m app.main
```

---

### 3. Test API

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

Example request:

```json
{
  "product_name": "Laptop",
  "quantity": 6
}
```

---

## 🧪 Testing

### Unit Tests

The project includes unit tests for:

- Payment success and failure flows
- Inventory reservation and release logic
- Order completion and cancellation handling
- Notification creation logic

### Integration Tests

Integration tests validate the complete Saga workflow across services:

- Successful order processing flow
- Compensation flow on payment failure

Run all integration tests:

```bash
pytest tests
```

---

## 🏗️ Architecture Highlights

- Event-driven communication using Kafka
- Saga pattern for distributed transactions
- Decoupled services with asynchronous workflows
- Independently deployable service structure
- Designed for scalability and fault tolerance

---

## 🛡️ Reliability Features

- Standardized event envelope across all services
- Correlation ID support for distributed tracing
- Causation ID for event lineage tracking
- Idempotent event processing implemented in all services
- Duplicate events are safely detected and skipped
- Processed events are persisted in PostgreSQL
- Compensation (rollback) mechanism for failed transactions
- Event-driven state transitions between services

---

## 🧠 Design Decisions

- Chose event-driven architecture over synchronous HTTP to reduce coupling
- Implemented Saga pattern to manage distributed transactions without central coordination
- Used Kafka for reliable event streaming and asynchronous processing
- Designed services as independently executable units

---

## ⚖️ Trade-offs

- Eventual consistency instead of strong consistency
- Increased complexity due to microservices architecture
- Requires proper monitoring and observability in real-world systems

---

## ⚡ Key Concepts Demonstrated

- Distributed system design
- Event-driven architecture
- Service decoupling
- Eventually consistent systems
- Message-based communication
- Idempotency handling
- Saga compensation (rollback) pattern
- Integration testing for distributed workflows

---

## 🧪 Idempotency Test

Duplicate events can be tested by publishing the same `event_id` multiple times to Kafka topics.

Expected behavior:

- First event is processed successfully
- Second event with the same `event_id` is skipped
- Event processing is recorded in PostgreSQL

---

## 🚧 Current Limitations

- No retry mechanism implemented
- No dead letter queue (DLQ)
- No centralized logging
- No distributed tracing system (e.g., OpenTelemetry)

---

## 🚀 Future Improvements

- Retry mechanisms and DLQ
- Transactional Outbox Pattern
- Observability (Prometheus + Grafana)
- Distributed tracing (OpenTelemetry)
- CI/CD pipeline integration
- Kubernetes deployment support

---

## 📦 Project Structure

```text
services/
  ├── order-service/
  ├── inventory-service/
  ├── payment-service/
  ├── shipping-service/
  └── notification-service/

infra/
  └── docker-compose.yml

docs/
  └── saga-flow.md

tests/
  └── integration tests
```

---

## 👨‍💻 Author

**Atakan Avsever**

- GitHub: https://github.com/Atakan-Avs
- LinkedIn: https://linkedin.com/in/atakanavsever

---

## ⭐️ Notes

This project is built as a portfolio-level backend system to demonstrate production-grade distributed architecture concepts including Saga orchestration, compensation transactions, idempotent event processing, and integration testing for asynchronous workflows.