"""
Оркестратор миссии команды М2.

Реализует полный цикл назначения дрона на заказ:

    1. Регистрация дрона в ОрВД
    2. Регистрация миссии в ОрВД
    3. Расчёт страховой премии (CALCULATION)
    4. Покупка страхового полиса (PURCHASE)
    5. Отправка миссии в НУС (task.submit + task.assign)

Все вызовы graceful: если сервис недоступен — логируем и продолжаем.
"""
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from .kafka_rpc import kafka_request
from .logger import log_event

logger = logging.getLogger(__name__)

# ── Топики (все настраиваются через .env) ──────────────────────────────────
ORVD_TOPIC = os.environ.get("ORVD_TOPIC", "v1.ORVD.ORVD001.main")
INSURER_REQUESTS_TOPIC = os.environ.get(
    "INSURER_REQUESTS_TOPIC", "v1.Insurer.1.insurer-service.requests"
)
INSURER_RESPONSES_TOPIC = os.environ.get(
    "INSURER_RESPONSES_TOPIC", "v1.Insurer.1.insurer-service.responses"
)
GCS_TOPIC = os.environ.get("GCS_TOPIC", "v1.gcs.1.orchestrator")

M2_OPERATOR_ID = os.environ.get("M2_OPERATOR_ID", "m2_operator")


# ── Вспомогательные функции ────────────────────────────────────────────────

def _parse_coords(point: str) -> tuple[float, float]:
    """
    Пытается распарсить координаты из строки.
    Форматы: "55.7558,37.6173" или просто "Казань" (тогда возвращает 0,0).
    """
    try:
        parts = point.split(",")
        if len(parts) == 2:
            return float(parts[0].strip()), float(parts[1].strip())
    except (ValueError, AttributeError):
        pass
    return 0.0, 0.0


def _build_route(order: dict) -> list[dict]:
    """Строит маршрут из точек заказа."""
    dep_lat, dep_lon = _parse_coords(order["departure_point"])
    dst_lat, dst_lon = _parse_coords(order["destination_point"])
    return [
        {"lat": dep_lat, "lon": dep_lon},
        {"lat": dst_lat, "lon": dst_lon},
    ]


# ── ОрВД ──────────────────────────────────────────────────────────────────

def register_drone_in_orvd(drone: dict) -> bool:
    """Регистрирует дрон в ОрВД. Возвращает True если успешно."""
    response = kafka_request(
        topic=ORVD_TOPIC,
        message={
            "action": "register_drone",
            "payload": {
                "drone_id": drone["serial_number"],
                "model": drone["name"],
                "operator": M2_OPERATOR_ID,
                "additional_info": {
                    "drone_type": drone["drone_type"],
                    "payload_capacity": drone["payload_capacity"],
                    "range_km": drone["range_km"],
                },
            },
        },
        timeout=8.0,
    )
    if response and response.get("payload", {}).get("status") in ("registered", "already_registered"):
        log_event("orvd_drone_registered", f"Дрон {drone['serial_number']} зарегистрирован в ОрВД.")
        return True
    if response is None:
        log_event("orvd_drone_register_skipped", f"ОрВД недоступен, регистрация дрона {drone['serial_number']} пропущена.")
    else:
        log_event("orvd_drone_register_failed", f"ОрВД отказал при регистрации дрона {drone['serial_number']}: {response}")
    return False


def register_mission_in_orvd(order: dict, drone: dict, mission_id: str) -> Optional[str]:
    """
    Регистрирует миссию в ОрВД.
    Возвращает mission_id если успешно, None если отклонено или недоступно.
    """
    response = kafka_request(
        topic=ORVD_TOPIC,
        message={
            "action": "register_mission",
            "payload": {
                "mission_id": mission_id,
                "drone_id": drone["serial_number"],
                "route": _build_route(order),
                "time": datetime.now(timezone.utc).isoformat(),
                "velocity": 10.0,
            },
        },
        timeout=8.0,
    )
    if response and response.get("payload", {}).get("status") == "mission_registered":
        log_event("orvd_mission_registered", f"Миссия {mission_id} зарегистрирована в ОрВД.")
        return mission_id
    if response is None:
        log_event("orvd_mission_skipped", f"ОрВД недоступен, миссия {mission_id} не зарегистрирована.")
        return mission_id  # продолжаем без ОрВД
    reason = response.get("payload", {}).get("reason", "unknown")
    log_event("orvd_mission_rejected", f"ОрВД отклонил миссию {mission_id}: {reason}")
    return None


# ── Страховая ─────────────────────────────────────────────────────────────

def purchase_insurance(order: dict, drone: dict) -> Optional[str]:
    """
    Рассчитывает и покупает страховой полис.
    Возвращает policy_id или None.
    """
    request_id = f"ins-{uuid.uuid4().hex[:12]}"
    coverage = float(order.get("offered_price") or 5_000_000)

    base = {
        "request_id": request_id,
        "order_id": str(order.get("external_request_id") or order["id"]),
        "manufacturer_id": "m2_manufacturer",
        "operator_id": M2_OPERATOR_ID,
        "drone_id": drone["serial_number"],
        "security_goals": ["ЦБ1", "ЦБ2"],
        "coverage_amount": coverage,
        "calculation_id": None,
        "incident": None,
    }

    # Шаг 1: CALCULATION
    calc_response = kafka_request(
        topic=INSURER_REQUESTS_TOPIC,
        message={**base, "request_type": "CALCULATION"},
        reply_topic=INSURER_RESPONSES_TOPIC,
        timeout=8.0,
    )

    if calc_response is None:
        log_event("insurance_skipped", f"Страховая недоступна, полис для дрона {drone['serial_number']} не оформлен.")
        return None

    calc_payload = calc_response.get("payload", calc_response)
    if calc_payload.get("status") != "SUCCESS":
        log_event("insurance_calc_failed", f"Страховая отказала в расчёте: {calc_payload.get('message')}")
        return None

    calculated_cost = calc_payload.get("calculated_cost", 0)
    calc_id = calc_payload.get("response_id", request_id)
    log_event("insurance_calculated", f"Страховая рассчитала премию: {calculated_cost} руб.")

    # Шаг 2: PURCHASE
    purchase_response = kafka_request(
        topic=INSURER_REQUESTS_TOPIC,
        message={**base, "calculation_id": calc_id, "request_type": "PURCHASE"},
        reply_topic=INSURER_RESPONSES_TOPIC,
        timeout=8.0,
    )

    if purchase_response is None:
        log_event("insurance_purchase_failed", "Страховая не ответила на запрос покупки полиса.")
        return None

    purchase_payload = purchase_response.get("payload", purchase_response)
    policy_id = purchase_payload.get("policy_id")
    if policy_id:
        log_event("insurance_purchased", f"Куплен страховой полис: {policy_id}")
        return policy_id

    log_event("insurance_purchase_failed", f"Страховая не выдала полис: {purchase_payload.get('message')}")
    return None


# ── НУС / GCS ─────────────────────────────────────────────────────────────

def submit_mission_to_gcs(order: dict, drone: dict, mission_id: str) -> Optional[str]:
    """
    Отправляет миссию в НУС (GCS):
      1. task.submit — создаёт задачу с маршрутом
      2. task.assign — назначает задачу на дрон

    Возвращает gcs_mission_id или None.
    """
    route = _build_route(order)
    waypoints = [{"lat": p["lat"], "lon": p["lon"], "alt": 100} for p in route]

    # task.submit
    submit_response = kafka_request(
        topic=GCS_TOPIC,
        message={
            "action": "task.submit",
            "payload": {
                "task": {"waypoints": waypoints},
                "mission_id": mission_id,
            },
        },
        timeout=8.0,
    )

    if submit_response is None:
        log_event("gcs_skipped", f"НУС недоступен, миссия {mission_id} не отправлена.")
        return None

    submit_payload = submit_response.get("payload", {})
    if not submit_response.get("success", False):
        log_event("gcs_submit_failed", f"НУС отказал в создании задачи: {submit_payload}")
        return None

    gcs_mission_id = submit_payload.get("mission_id", mission_id)
    log_event("gcs_mission_submitted", f"Миссия {gcs_mission_id} принята НУС.")

    # task.assign
    assign_response = kafka_request(
        topic=GCS_TOPIC,
        message={
            "action": "task.assign",
            "payload": {
                "mission_id": gcs_mission_id,
                "drone_id": drone["serial_number"],
            },
        },
        timeout=8.0,
    )

    if assign_response and assign_response.get("success"):
        log_event("gcs_mission_assigned", f"Дрон {drone['serial_number']} назначен на задачу {gcs_mission_id} в НУС.")
    else:
        log_event("gcs_assign_failed", f"НУС не назначил дрон на задачу {gcs_mission_id}.")

    return gcs_mission_id


# ── Главная функция ────────────────────────────────────────────────────────

def run_mission_assignment(order: dict, drone: dict) -> dict:
    """
    Полный цикл назначения дрона на миссию.

    Возвращает dict с результатами каждого шага:
    {
        "mission_id": str,
        "orvd_ok": bool,
        "insurance_policy_id": str | None,
        "gcs_task_id": str | None,
    }
    """
    mission_id = f"MSN-M2-{uuid.uuid4().hex[:8].upper()}"
    result = {
        "mission_id": mission_id,
        "orvd_ok": False,
        "insurance_policy_id": None,
        "gcs_task_id": None,
    }

    log_event("mission_assignment_started", f"Начало назначения миссии {mission_id} для заказа ID={order['id']}.")

    # 1. ОрВД: регистрация дрона
    register_drone_in_orvd(drone)

    # 2. ОрВД: регистрация миссии
    orvd_result = register_mission_in_orvd(order, drone, mission_id)
    result["orvd_ok"] = orvd_result is not None

    # 3. Страховая
    policy_id = purchase_insurance(order, drone)
    result["insurance_policy_id"] = policy_id

    # 4. НУС
    gcs_task_id = submit_mission_to_gcs(order, drone, mission_id)
    result["gcs_task_id"] = gcs_task_id

    log_event(
        "mission_assignment_completed",
        f"Миссия {mission_id}: ОрВД={result['orvd_ok']}, "
        f"страховка={policy_id or 'нет'}, НУС={gcs_task_id or 'нет'}.",
    )
    return result
