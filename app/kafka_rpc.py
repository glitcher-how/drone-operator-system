"""
Синхронный Kafka request-reply.

Отправляет сообщение в топик и ждёт ответ по correlation_id.
Используется для вызовов ОрВД, Страховой, НУС из Flask-обработчиков.
"""
import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from kafka import KafkaConsumer, KafkaProducer

logger = logging.getLogger(__name__)

M2_REPLY_BASE = os.environ.get("M2_REPLY_BASE", "v1.replies.m2_operator")


def _sasl_config() -> dict:
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


def kafka_request(
    topic: str,
    message: dict,
    timeout: float = 10.0,
    reply_topic: Optional[str] = None,
) -> Optional[dict]:
    """
    Отправляет message в topic, ждёт ответ с matching correlation_id.

    Возвращает dict с ответом или None при таймауте / ошибке.
    """
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        logger.warning(f"kafka_request: KAFKA_BOOTSTRAP_SERVERS не задан, пропускаем запрос в {topic}")
        return None

    sasl = _sasl_config()
    correlation_id = str(uuid.uuid4())

    if reply_topic is None:
        reply_topic = f"{M2_REPLY_BASE}.{uuid.uuid4().hex[:8]}"

    full_message = {
        **message,
        "correlation_id": correlation_id,
        "reply_to": reply_topic,
        "sender": "m2_operator",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    result_holder: dict = {}
    event = threading.Event()

    def consume_reply():
        try:
            consumer = KafkaConsumer(
                reply_topic,
                bootstrap_servers=servers.split(","),
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                group_id=f"mk2_rpc_{uuid.uuid4().hex[:8]}",
                consumer_timeout_ms=int(timeout * 1000),
                **sasl,
            )
            for msg in consumer:
                data = msg.value
                if data.get("correlation_id") == correlation_id:
                    result_holder["response"] = data
                    event.set()
                    break
            consumer.close()
        except Exception as e:
            logger.error(f"kafka_request consumer error: {e}")
            event.set()

    # Запускаем consumer в отдельном потоке
    t = threading.Thread(target=consume_reply, daemon=True)
    t.start()

    # Небольшая пауза чтобы consumer успел подписаться
    import time
    time.sleep(0.5)

    try:
        producer = KafkaProducer(
            bootstrap_servers=servers.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            **sasl,
        )
        producer.send(topic, full_message)
        producer.flush()
        producer.close()
        logger.info(f"kafka_request → {topic} | action={message.get('action') or message.get('request_type')} | corr={correlation_id[:8]}")
    except Exception as e:
        logger.error(f"kafka_request send error: {e}")
        return None

    event.wait(timeout=timeout)
    response = result_holder.get("response")
    if response is None:
        logger.warning(f"kafka_request timeout ({timeout}s) waiting for response from {topic}")
    return response
