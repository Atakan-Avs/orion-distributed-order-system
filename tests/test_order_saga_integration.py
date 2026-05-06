import time

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


def get_order_status(order_id: int) -> str:
    connection = psycopg2.connect(**DB_CONFIG)

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT status FROM orders WHERE id = %s",
                (order_id,),
            )
            row = cursor.fetchone()
            return row[0] if row else None

    finally:
        connection.close()


def wait_for_order_status(order_id: int, expected_status: str, timeout_seconds: int = 10):
    start_time = time.time()

    while time.time() - start_time < timeout_seconds:
        status = get_order_status(order_id)

        if status == expected_status:
            return status

        time.sleep(0.5)

    raise AssertionError(
        f"Order {order_id} did not reach status {expected_status} within {timeout_seconds}s"
    )


def test_successful_order_saga_flow_completes_order():
    response = requests.post(
        API_URL,
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


def test_payment_failure_triggers_compensation_and_cancels_order():
    response = requests.post(
        API_URL,
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