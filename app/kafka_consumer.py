"""
Kafka consumer команды М2.

Подписывается на:
  1. v1.aggregator_insurer.local.operator.requests
     — заказы от Aggregator (create_order, confirm_price)
  2. M2_REPLY_TOPIC (v1.replies.m2_operator.droneport)
     — ответы от DronePort на get_available_drones

Периодически запрашивает доступные дроны из DronePort.
"""
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger(__name__)


def _sasl_config() -> dict:
    """SASL PLAIN аутентификация из переменных окружения BROKER_USER / BROKER_PASSWORD."""
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

# Топики (настраиваются через .env)
AGGREGATOR_REQUESTS_TOPIC = os.environ.get(
    "AGGREGATOR_REQUESTS_TOPIC",
    "v1.aggregator_insurer.local.operator.requests",
)
DRONEPORT_REGISTRY_TOPIC = os.environ.get(
    "DRONEPORT_REGISTRY_TOPIC",
    "v1.drone_port.1.registry",
)
M2_REPLY_TOPIC = os.environ.get(
    "M2_REPLY_TOPIC",
    "v1.replies.m2_operator.droneport",
)
DRONEPORT_SYNC_INTERVAL = int(os.environ.get("DRONEPORT_SYNC_INTERVAL", "60"))


# ---------------------------------------------------------------------------
# DronePort — обработка ответов
# ---------------------------------------------------------------------------

def _upsert_drone_from_droneport(data: dict) -> None:
    """
    Вставляет или обновляет дрон, полученный от DronePort.

    DronePort поля:
      drone_id, model, battery (str), status (new/ready/charging/busy)
    """
    from .db import execute, query_one

    drone_id = data.get("drone_id")
    if not drone_id:
        return

    status_map = {
        "ready": "ready",
        "charging": "maintenance",
        "busy": "busy",
        "new": "registered",
    }
    try:
        battery = int(float(data.get("battery", 100)))
    except (ValueError, TypeError):
        battery = 100
    battery = max(0, min(100, battery))

    m2_status = status_map.get(data.get("status", "ready"), "ready")
    name = data.get("model", drone_id)

    existing = query_one(
        "SELECT id FROM drones WHERE serial_number = ?", (drone_id,)
    )
    if existing:
        execute(
            """UPDATE drones SET name = ?, status = ?, battery_level = ?
               WHERE serial_number = ?""",
            (name, m2_status, battery, drone_id),
        )
    else:
        execute(
            """INSERT INTO drones(
                name, drone_type, serial_number, status,
                payload_capacity, range_km, battery_level, certificate_valid_until
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, "external", drone_id, m2_status, 0.0, 0.0, battery, "2099-12-31"),
        )


# ---------------------------------------------------------------------------
# Aggregator — обработка входящих заказов
# ---------------------------------------------------------------------------

def _handle_create_order(payload: dict, request_id: str) -> None:
    """
    Получили create_order от Aggregator — создаём заказ в локальной БД.

    Aggregator payload:
      customer_id, description, budget, mission_type,
      from_lat, from_lon, to_lat, to_lon
    """
    from .db import execute, query_one

    existing = query_one(
        "SELECT id FROM orders WHERE external_request_id = ?", (request_id,)
    )
    if existing:
        return  # уже создан

    departure = f"{payload.get('from_lat', 0)},{payload.get('from_lon', 0)}"
    destination = f"{payload.get('to_lat', 0)},{payload.get('to_lon', 0)}"
    budget = float(payload.get("budget", 0))
    # Бюджет используем как приблизительный вес груза (заглушка)
    cargo_weight = round(budget / 1000, 2) if budget > 0 else 1.0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    execute(
        """INSERT INTO orders(
            customer_name, mission_type, cargo_weight, departure_point,
            destination_point, required_time, status, created_at,
            source, external_request_id, offered_price
           ) VALUES (?, ?, ?, ?, ?, ?, 'new', ?, 'aggregator', ?, ?)""",
        (
            payload.get("customer_id", "Aggregator"),
            payload.get("mission_type", "delivery"),
            cargo_weight,
            departure,
            destination,
            now,
            now,
            request_id,
            budget,
        ),
    )
    logger.info(f"[Aggregator] Создан заказ: request_id={request_id}")


def _handle_confirm_price(payload: dict, request_id: str) -> None:
    """
    Aggregator подтвердил цену — переводим заказ в assigned (если ещё нет дрона)
    или in_progress (если дрон уже назначен).
    """
    from .db import execute, query_one

    order = query_one(
        "SELECT * FROM orders WHERE external_request_id = ?", (request_id,)
    )
    if not order:
        logger.warning(f"[Aggregator] confirm_price: заказ {request_id} не найден")
        return

    if order["status"] == "new":
        execute(
            "UPDATE orders SET status = 'in_progress' WHERE external_request_id = ?",
            (request_id,),
        )
    logger.info(f"[Aggregator] Цена подтверждена: request_id={request_id}")


# ---------------------------------------------------------------------------
# DronePort — периодический запрос доступных дронов
# ---------------------------------------------------------------------------

def _request_droneport_drones(producer: KafkaProducer) -> None:
    message = {
        "action": "get_available_drones",
        "payload": {},
        "sender": "m2_operator",
        "correlation_id": str(uuid.uuid4()),
        "reply_to": M2_REPLY_TOPIC,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        producer.send(DRONEPORT_REGISTRY_TOPIC, message)
        producer.flush()
        logger.info(f"[DronePort] Запрос get_available_drones → {DRONEPORT_REGISTRY_TOPIC}")
    except Exception as e:
        logger.error(f"[DronePort] Ошибка отправки запроса: {e}")


def _schedule_sync(producer: KafkaProducer) -> None:
    _request_droneport_drones(producer)
    timer = threading.Timer(
        DRONEPORT_SYNC_INTERVAL,
        _schedule_sync,
        args=(producer,),
    )
    timer.daemon = True
    timer.start()


# ---------------------------------------------------------------------------
# Основной цикл
# ---------------------------------------------------------------------------

def _consume_loop(bootstrap_servers: str, app) -> None:
    topics = [AGGREGATOR_REQUESTS_TOPIC, M2_REPLY_TOPIC]

    sasl = _sasl_config()
    try:
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers.split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id="mk2_consumer",
            **sasl,
        )
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            acks="all",
            **sasl,
        )
    except Exception as e:
        logger.error(f"Не удалось подключиться к Kafka: {e}")
        return

    logger.info(f"Kafka consumer подключён. Топики: {topics}")

    # Первый запрос в DronePort + запуск периодической синхронизации
    _schedule_sync(producer)

    with app.app_context():
        for message in consumer:
            try:
                data = message.value
                topic = message.topic

                if topic == AGGREGATOR_REQUESTS_TOPIC:
                    msg_type = data.get("type")
                    request_id = data.get("request_id", "")
                    payload = data.get("payload", {})

                    if msg_type == "create_order":
                        _handle_create_order(payload, request_id)
                    elif msg_type == "confirm_price":
                        _handle_confirm_price(payload, request_id)
                    else:
                        logger.debug(
                            f"[Aggregator] Неизвестный тип: '{msg_type}'"
                        )

                elif topic == M2_REPLY_TOPIC:
                    if data.get("success"):
                        drones = data.get("payload", {}).get("drones", [])
                        for drone in drones:
                            _upsert_drone_from_droneport(drone)
                        logger.info(
                            f"[DronePort] Синхронизировано дронов: {len(drones)}"
                        )
                    else:
                        logger.warning(
                            f"[DronePort] Ошибка в ответе: {data.get('payload')}"
                        )

            except Exception as e:
                logger.error(f"Ошибка обработки Kafka-сообщения: {e}")


def start_consumer(bootstrap_servers: str, app) -> None:
    thread = threading.Thread(
        target=_consume_loop,
        args=(bootstrap_servers, app),
        daemon=True,
        name="kafka-consumer",
    )
    thread.start()
    logger.info("Kafka consumer запущен в фоновом потоке.")
