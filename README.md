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

---

## 🧩 Services

- **order-service** → creates orders and starts the Saga
- **inventory-service** → reserves stock
- **payment-service** → processes payment
- **shipping-service** → creates shipment
- **notification-service** → sends notifications

---

## 🔁 Saga Flow

OrderCreated → InventoryReserved → PaymentCompleted → ShippingCreated → NotificationSent

Detailed flow: docs/saga-flow.md

---

## ▶️ How to Run

### 1. Start infrastructure

cd infra  
docker compose up -d  

---

### 2. Start services (each in separate terminal)

Order Service  
cd services/order-service  
uvicorn app.main:app --reload  

Inventory Service  
cd services/inventory-service  
python -m app.main  

Payment Service  
cd services/payment-service  
python -m app.main  

Shipping Service  
cd services/shipping-service  
python -m app.main  

Notification Service  
cd services/notification-service  
python -m app.main  

---

### 3. Test API

Swagger UI:  
http://127.0.0.1:8000/docs  

Example request:

{  
  "product_name": "Laptop",  
  "quantity": 1  
}

---

## 🏗️ Architecture Highlights

- Event-driven communication using Kafka
- Saga pattern for distributed transactions
- Decoupled services with asynchronous workflows
- Independently deployable service structure
- Designed for scalability and fault tolerance

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

---

## 🚧 Current Limitations

- No retry mechanism implemented
- No dead letter queue (DLQ)
- No idempotency handling
- No centralized logging

---

## 🚀 Future Improvements

- Payment failure & compensation flow
- Inventory rollback
- Transactional Outbox Pattern
- Idempotency handling
- Retry mechanisms and DLQ
- Observability (Prometheus + Grafana)

---

## 📦 Project Structure

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

---

## 👨‍💻 Author

Atakan Avsever  
GitHub: https://github.com/Atakan-Avs  
LinkedIn: https://linkedin.com/in/atakanavsever  

---

## ⭐️ Notes

This project is built as a portfolio-level backend system to demonstrate production-grade architecture concepts.