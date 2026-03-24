"""
Модуль получения доступных дронов.

Раньше делал HTTP-запрос к внешнему DronePort API.
Теперь данные о дронах других команд приходят через Kafka (kafka_consumer.py)
и хранятся в локальной БД. Этот модуль просто читает из БД.
"""
from .db import query_all
from .logger import log_event


def get_available_drones() -> list[str]:
    """
    Возвращает серийные номера всех дронов со статусом 'ready' из локальной БД.
    БД обновляется Kafka consumer'ом в реальном времени из топика drone_registry.
    """
    try:
        rows = query_all(
            "SELECT serial_number FROM drones WHERE status = 'ready'"
        )
        serials = [row["serial_number"] for row in rows]

        log_event(
            "available_drones_queried",
            f"Из локальной БД (Kafka) получены доступные дроны: {', '.join(serials) or 'нет'}.",
        )

        return serials

    except Exception as e:
        log_event(
            "available_drones_error",
            f"Ошибка при получении списка дронов из БД: {str(e)}.",
        )
        return []
