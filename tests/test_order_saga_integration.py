import time
from uuid import uuid4

import psycopg2
import requests


API_URL = "http://127.0.0.1:8000/orders"

DB_CONFIG = {
    "host": "localhost",
    "port": 5435,
    "database": "orion_db",
    "user": "orion_user",
    "password": "orion_password",
}


def fetch_one(query: str, params: tuple):
    connection = psycopg2.connect(**DB_CONFIG)

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()

    finally:
        connection.close()


def get_order_status(order_id: int) -> str:
    row = fetch_one(
        "SELECT status FROM orders WHERE id = %s",
        (order_id,),
    )

    return row[0] if row else None


def get_outbox_event_by_correlation_id(
    table_name: str,
    event_type: str,
    correlation_id: str,
):
    query = f"""
        SELECT payload
        FROM {table_name}
        WHERE event_type = %s
          AND payload->>'correlation_id' = %s
        ORDER BY id DESC
        LIMIT 1
    """

    row = fetch_one(
        query,
        (
            event_type,
            correlation_id,
        ),
    )

    return row[0] if row else None


def wait_for_order_status(
    order_id: int,
    expected_status: str,
    timeout_seconds: int = 15,
):
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        status = get_order_status(order_id)

        if status == expected_status:
            return status

        time.sleep(0.5)

    raise AssertionError(
        f"Order {order_id} did not reach status {expected_status} "
        f"within {timeout_seconds}s"
    )


def wait_for_outbox_event(
    table_name: str,
    event_type: str,
    correlation_id: str,
    timeout_seconds: int = 15,
):
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        event = get_outbox_event_by_correlation_id(
            table_name=table_name,
            event_type=event_type,
            correlation_id=correlation_id,
        )

        if event is not None:
            return event

        time.sleep(0.5)

    raise AssertionError(
        f"Event {event_type} with correlation_id={correlation_id} "
        f"was not found in {table_name} within {timeout_seconds}s"
    )


def assert_event_metadata(
    event: dict,
    expected_event_type: str,
    expected_source: str,
    expected_correlation_id: str,
):
    assert event["event_type"] == expected_event_type
    assert event["source"] == expected_source
    assert event["correlation_id"] == expected_correlation_id
    assert "event_id" in event
    assert "occurred_at" in event


def test_successful_order_saga_flow_completes_order_and_preserves_correlation_id():
    correlation_id = f"e2e-success-{uuid4()}"

    response = requests.post(
        API_URL,
        headers={
            "X-Correlation-ID": correlation_id,
        },
        json={
            "product_name": "Integration Laptop",
            "quantity": 2,
        },
        timeout=5,
    )

    assert response.status_code == 200

    order = response.json()
    order_id = order["id"]

    final_status = wait_for_order_status(order_id, "COMPLETED")

    assert final_status == "COMPLETED"

    order_created_event = wait_for_outbox_event(
        table_name="outbox_events",
        event_type="OrderCreated",
        correlation_id=correlation_id,
    )

    inventory_reserved_event = wait_for_outbox_event(
        table_name="inventory_outbox_events",
        event_type="InventoryReserved",
        correlation_id=correlation_id,
    )

    payment_completed_event = wait_for_outbox_event(
        table_name="payment_outbox_events",
        event_type="PaymentCompleted",
        correlation_id=correlation_id,
    )

    shipping_created_event = wait_for_outbox_event(
        table_name="shipping_outbox_events",
        event_type="ShippingCreated",
        correlation_id=correlation_id,
    )

    notification_sent_event = wait_for_outbox_event(
        table_name="notification_outbox_events",
        event_type="NotificationSent",
        correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=order_created_event,
        expected_event_type="OrderCreated",
        expected_source="order-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=inventory_reserved_event,
        expected_event_type="InventoryReserved",
        expected_source="inventory-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=payment_completed_event,
        expected_event_type="PaymentCompleted",
        expected_source="payment-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=shipping_created_event,
        expected_event_type="ShippingCreated",
        expected_source="shipping-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=notification_sent_event,
        expected_event_type="NotificationSent",
        expected_source="notification-service",
        expected_correlation_id=correlation_id,
    )

    assert order_created_event["payload"]["order_id"] == order_id
    assert inventory_reserved_event["payload"]["order_id"] == order_id
    assert payment_completed_event["payload"]["order_id"] == order_id
    assert shipping_created_event["payload"]["order_id"] == order_id
    assert notification_sent_event["payload"]["order_id"] == order_id

    assert inventory_reserved_event["causation_id"] == order_created_event["event_id"]
    assert payment_completed_event["causation_id"] == inventory_reserved_event["event_id"]
    assert shipping_created_event["causation_id"] == payment_completed_event["event_id"]
    assert notification_sent_event["causation_id"] == shipping_created_event["event_id"]


def test_payment_failure_triggers_compensation_and_preserves_correlation_id():
    correlation_id = f"e2e-failure-{uuid4()}"

    response = requests.post(
        API_URL,
        headers={
            "X-Correlation-ID": correlation_id,
        },
        json={
            "product_name": "Integration Laptop",
            "quantity": 6,
        },
        timeout=5,
    )

    assert response.status_code == 200

    order = response.json()
    order_id = order["id"]

    final_status = wait_for_order_status(order_id, "CANCELLED")

    assert final_status == "CANCELLED"

    order_created_event = wait_for_outbox_event(
        table_name="outbox_events",
        event_type="OrderCreated",
        correlation_id=correlation_id,
    )

    inventory_reserved_event = wait_for_outbox_event(
        table_name="inventory_outbox_events",
        event_type="InventoryReserved",
        correlation_id=correlation_id,
    )

    payment_failed_event = wait_for_outbox_event(
        table_name="payment_outbox_events",
        event_type="PaymentFailed",
        correlation_id=correlation_id,
    )

    inventory_released_event = wait_for_outbox_event(
        table_name="inventory_outbox_events",
        event_type="InventoryReleased",
        correlation_id=correlation_id,
    )

    order_cancelled_event = wait_for_outbox_event(
        table_name="outbox_events",
        event_type="OrderCancelled",
        correlation_id=correlation_id,
    )

    notification_sent_event = wait_for_outbox_event(
        table_name="notification_outbox_events",
        event_type="NotificationSent",
        correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=order_created_event,
        expected_event_type="OrderCreated",
        expected_source="order-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=inventory_reserved_event,
        expected_event_type="InventoryReserved",
        expected_source="inventory-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=payment_failed_event,
        expected_event_type="PaymentFailed",
        expected_source="payment-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=inventory_released_event,
        expected_event_type="InventoryReleased",
        expected_source="inventory-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=order_cancelled_event,
        expected_event_type="OrderCancelled",
        expected_source="order-service",
        expected_correlation_id=correlation_id,
    )

    assert_event_metadata(
        event=notification_sent_event,
        expected_event_type="NotificationSent",
        expected_source="notification-service",
        expected_correlation_id=correlation_id,
    )

    assert order_created_event["payload"]["order_id"] == order_id
    assert inventory_reserved_event["payload"]["order_id"] == order_id
    assert payment_failed_event["payload"]["order_id"] == order_id
    assert inventory_released_event["payload"]["order_id"] == order_id
    assert order_cancelled_event["payload"]["order_id"] == order_id
    assert notification_sent_event["payload"]["order_id"] == order_id

    assert inventory_reserved_event["causation_id"] == order_created_event["event_id"]
    assert payment_failed_event["causation_id"] == inventory_reserved_event["event_id"]
    assert inventory_released_event["causation_id"] == payment_failed_event["event_id"]
    assert order_cancelled_event["causation_id"] == inventory_released_event["event_id"]
    assert notification_sent_event["causation_id"] == order_cancelled_event["event_id"]