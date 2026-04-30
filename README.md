# ORION - Distributed Order System 🚀

ORION is a production-oriented distributed order management system built with a microservices architecture using the Saga pattern and event-driven communication.

This project demonstrates how real-world scalable backend systems are designed and implemented.

---

## 🧠 Architecture Overview

ORION follows an event-driven microservices architecture:

- Each service is independent
- Services communicate via Kafka
- No direct HTTP communication between services
- Uses Saga pattern for distributed transactions

---

## ⚙️ Tech Stack

- FastAPI
- PostgreSQL
- Kafka
- Redis
- Docker Compose
- SQLAlchemy
- Python

---

## 🧩 Services

| Service | Responsibility |
|--------|--------------|
| order-service | Creates orders and starts Saga |
| inventory-service | Reserves stock |
| payment-service | Processes payment |
| shipping-service | Creates shipment |
| notification-service | Sends notification |

---

## 🔁 Saga Flow

OrderCreated → InventoryReserved → PaymentCompleted → ShippingCreated → NotificationSent

Detailed flow: `docs/saga-flow.md`

## ▶️ How to Run
1. Start infrastructure
cd infra
docker compose up -d
2. Start services (each in separate terminal)
# Order Service
cd services/order-service
uvicorn app.main:app --reload
# Inventory Service
cd services/inventory-service
python -m app.main
# Payment Service
cd services/payment-service
python -m app.main
# Shipping Service
cd services/shipping-service
python -m app.main
# Notification Service
cd services/notification-service
python -m app.main
3. Test API

Open Swagger:

http://127.0.0.1:8000/docs

Create order:

{
  "product_name": "Laptop",
  "quantity": 1
}
🧪 What This Project Demonstrates
Microservices architecture
Event-driven communication
Kafka messaging
Saga pattern (distributed transaction)
Service decoupling
Real-world backend system design
🚀 Future Improvements
Payment failure & compensation flow
Inventory rollback
Transactional Outbox Pattern
Idempotency handling
Retry mechanisms
Dead Letter Queue (DLQ)
Observability (Prometheus + Grafana)
👨‍💻 Author

Atakan Avsever

GitHub: https://github.com/Atakan-Avs
LinkedIn: https://linkedin.com/in/atakanavsever
⭐️ Notes

This project is designed as a portfolio-level backend system to demonstrate production-grade architecture concepts.