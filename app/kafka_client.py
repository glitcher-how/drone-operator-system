import json
import os
from dataclasses import dataclass

from kafka import KafkaProducer

AGGREGATOR_RESPONSES_TOPIC = os.environ.get(
    "AGGREGATOR_RESPONSES_TOPIC",
    "v1.aggregator_insurer.local.operator.responses",
)
M2_OPERATOR_ID = os.environ.get("M2_OPERATOR_ID", "m2_operator")
M2_OPERATOR_NAME = os.environ.get("M2_OPERATOR_NAME", "M2 Drone Operator")


def _sasl_config() -> dict:
    """SASL PLAIN аутентификация — берётся из BROKER_USER / BROKER_PASSWORD."""
    user = os.environ.get("BROKER_USER")
    password = os.environ.get("BROKER_PASSWORD")
    if user and password:
        return {
            "security_protocol": "SASL_PLAINTEXT",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": user,
            "sasl_plain_password": password,
        }
    return {}


@dataclass
class KafkaConfig:
    bootstrap_servers: str
    topic: str


def get_kafka_config() -> KafkaConfig | None:
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.environ.get("KAFKA_TOPIC", "drone_events")

    if not servers:
        return None

    return KafkaConfig(bootstrap_servers=servers, topic=topic)


class KafkaClient:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all",
            **_sasl_config(),
        )

    def publish(self, event_type: str, data: dict) -> None:
        payload = {
            "event_type": event_type,
            "data": data,
        }
        self.producer.send(self.config.topic, payload)
        self.producer.flush()

    def publish_drone(self, drone_data: dict) -> None:
        """Публикует данные дрона в топик drone_registry для других команд."""
        self.producer.send("drone_registry", drone_data)
        self.producer.flush()

    def publish_droneport(self, droneport_data: dict) -> None:
        """Публикует данные дронопорта в топик droneport_registry для других команд."""
        self.producer.send("droneport_registry", droneport_data)
        self.producer.flush()

    def publish_price_offer(
        self,
        request_id: str,
        order_id: int,
        price: float,
        estimated_minutes: int = 30,
        security_goals: list | None = None,
    ) -> None:
        """
        Отправляет price_offer в Aggregator.
        Топик: v1.aggregator_insurer.local.operator.responses
        """
        payload = {
            "request_id": request_id,
            "type": "price_offer",
            "payload": {
                "order_id": request_id,
                "operator_id": M2_OPERATOR_ID,
                "operator_name": M2_OPERATOR_NAME,
                "price": price,
                "estimated_time_minutes": estimated_minutes,
                "provided_security_goals": security_goals or ["ЦБ1"],
                "insurance_coverage": "Лимит 1 млн",
            },
        }
        self.producer.send(AGGREGATOR_RESPONSES_TOPIC, payload)
        self.producer.flush()

    def publish_order_result(
        self,
        request_id: str,
        success: bool,
        total_price: float,
        reason: str = "",
    ) -> None:
        """
        Отправляет order_result в Aggregator после завершения/отмены миссии.
        Топик: v1.aggregator_insurer.local.operator.responses
        """
        payload = {
            "request_id": request_id,
            "type": "order_result",
            "payload": {
                "order_id": request_id,
                "operator_id": M2_OPERATOR_ID,
                "success": success,
                "reason": reason,
                "total_price": total_price if success else 0.0,
            },
        }
        self.producer.send(AGGREGATOR_RESPONSES_TOPIC, payload)
        self.producer.flush()
