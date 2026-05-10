from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class OrderStatus:
    PENDING = "PENDING"
    INVENTORY_RESERVED = "INVENTORY_RESERVED"
    PAYMENT_COMPLETED = "PAYMENT_COMPLETED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True, index=True)
    product_name = Column(String, nullable=False)
    quantity = Column(Integer, nullable=False)
    status = Column(String, default=OrderStatus.PENDING)