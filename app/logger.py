import os
from datetime import datetime

from .db import execute
from .kafka_client import KafkaClient, get_kafka_config

kafka_client = None
kafka_config = get_kafka_config()
if kafka_config is not None:
    kafka_client = KafkaClient(kafka_config)


def log_event(event_type: str, details: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    execute(
        "INSERT INTO events(created_at, event_type, details) VALUES (?, ?, ?)",
        (timestamp, event_type, details),
    )

    if kafka_client is not None and os.environ.get(
        "ENABLE_KAFKA_EVENTS", "true"
    ).lower() in (
        "true",
        "1",
        "yes",
    ):
        kafka_client.publish(event_type, {"details": details, "created_at": timestamp})


def publish_drone(drone_data: dict) -> None:
    """Публикует дрон команды М2 в Kafka топик drone_registry."""
    if kafka_client is not None:
        kafka_client.publish_drone(drone_data)


def publish_droneport(droneport_data: dict) -> None:
    """Публикует дронопорт команды М2 в Kafka топик droneport_registry."""
    if kafka_client is not None:
        kafka_client.publish_droneport(droneport_data)


def publish_price_offer(
    request_id: str,
    order_id: int,
    price: float,
    estimated_minutes: int = 30,
) -> None:
    """Отправляет предложение цены в Aggregator."""
    if kafka_client is not None:
        kafka_client.publish_price_offer(
            request_id=request_id,
            order_id=order_id,
            price=price,
            estimated_minutes=estimated_minutes,
        )


def publish_order_result(
    request_id: str,
    success: bool,
    total_price: float,
    reason: str = "",
) -> None:
    """Отправляет результат миссии в Aggregator."""
    if kafka_client is not None:
        kafka_client.publish_order_result(
            request_id=request_id,
            success=success,
            total_price=total_price,
            reason=reason,
        )
