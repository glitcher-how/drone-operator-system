"""
Оркестратор миссии команды М2.

Полный цикл назначения дрона на заказ (по инструкции преподавателя):

    1. Регистрация дрона в ОрВД
    2. Регистрация миссии в ОрВД
    3. Расчёт страховой премии (CALCULATION)
    4. Покупка страхового полиса (PURCHASE)
    5. Отправка миссии в НУС (task.submit + task.assign)
    6. Запрос авторизации вылета в ОрВД (request_takeoff)

Отдельно: регистрация топиков М2 у Регулятора при старте системы.
Отдельно: отправка страхового инцидента при отмене/сбое миссии.

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

# ── Топики (настраиваются через .env) ──────────────────────────────────────
ORVD_TOPIC = os.environ.get("ORVD_TOPIC", "v1.ORVD.ORVD001.main")
INSURER_REQUESTS_TOPIC = os.environ.get(
    "INSURER_REQUESTS_TOPIC", "v1.Insurer.1.insurer-service.requests"
)
INSURER_RESPONSES_TOPIC = os.environ.get(
    "INSURER_RESPONSES_TOPIC", "v1.Insurer.1.insurer-service.responses"
)
GCS_TOPIC = os.environ.get("GCS_TOPIC", "v1.gcs.1.orchestrator")
REGULATOR_TOPIC = os.environ.get("REGULATOR_TOPIC", "v1.regulator.1.registration")

M2_OPERATOR_ID = os.environ.get("M2_OPERATOR_ID", "m2_operator")
M2_OPERATOR_NAME = os.environ.get("M2_OPERATOR_NAME", "M2 Drone Operator")


# ── Вспомогательные функции ────────────────────────────────────────────────

def _parse_coords(point: str) -> tuple[float, float]:
    try:
        parts = point.split(",")
        if len(parts) == 2:
            return float(parts[0].strip()), float(parts[1].strip())
    except (ValueError, AttributeError):
        pass
    return 0.0, 0.0


def _build_route(order: dict) -> list[dict]:
    dep_lat, dep_lon = _parse_coords(order["departure_point"])
    dst_lat, dst_lon = _parse_coords(order["destination_point"])
    return [
        {"lat": dep_lat, "lon": dep_lon},
        {"lat": dst_lat, "lon": dst_lon},
    ]


def _security_goals(order: dict) -> list[str]:
    """Берёт цели безопасности из заказа или возвращает минимальный набор."""
    stored = order.get("security_goals")
    if stored:
        import json
        try:
            return json.loads(stored)
        except Exception:
            pass
    return ["ЦБ1"]


# ── Регулятор ─────────────────────────────────────────────────────────────

def register_topics_with_regulator() -> None:
    """
    Регистрирует топики М2 у Регулятора при старте системы.
    Регулятор должен знать, куда слать сообщения эксплуатанту.
    """
    aggregator_requests = os.environ.get(
        "AGGREGATOR_REQUESTS_TOPIC", "v1.aggregator_insurer.local.operator.requests"
    )
    aggregator_responses = os.environ.get(
        "AGGREGATOR_RESPONSES_TOPIC", "v1.aggregator_insurer.local.operator.responses"
    )
    insurer_responses = INSURER_RESPONSES_TOPIC

    response = kafka_request(
        topic=REGULATOR_TOPIC,
        message={
            "action": "register_system",
            "payload": {
                "system_id": M2_OPERATOR_ID,
                "system_name": M2_OPERATOR_NAME,
                "system_type": "operator",
                "topics": {
                    "incoming_orders": aggregator_requests,
                    "outgoing_responses": aggregator_responses,
                    "insurance_responses": insurer_responses,
                },
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        },
        timeout=5.0,
    )
    if response:
        log_event("regulator_registered", f"М2 зарегистрирована у Регулятора.")
    else:
        log_event("regulator_skipped", "Регулятор недоступен при старте, регистрация пропущена.")


# ── ОрВД ──────────────────────────────────────────────────────────────────

def register_drone_in_orvd(drone: dict) -> bool:
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
    status = (response or {}).get("payload", {}).get("status", "")
    if status in ("registered", "already_registered"):
        log_event("orvd_drone_registered", f"Дрон {drone['serial_number']} зарегистрирован в ОрВД.")
        return True
    if response is None:
        log_event("orvd_drone_skipped", f"ОрВД недоступен, регистрация дрона пропущена.")
    else:
        log_event("orvd_drone_failed", f"ОрВД отказал при регистрации дрона: {response}")
    return False


def register_mission_in_orvd(order: dict, drone: dict, mission_id: str) -> bool:
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
    status = (response or {}).get("payload", {}).get("status", "")
    if response is None:
        log_event("orvd_mission_skipped", f"ОрВД недоступен, миссия {mission_id} не зарегистрирована.")
        return True  # продолжаем без ОрВД
    if status == "mission_registered":
        log_event("orvd_mission_registered", f"Миссия {mission_id} зарегистрирована в ОрВД.")
        return True
    reason = (response or {}).get("payload", {}).get("reason", "unknown")
    log_event("orvd_mission_rejected", f"ОрВД отклонил миссию {mission_id}: {reason}")
    return False


def request_takeoff_from_orvd(drone: dict, mission_id: str) -> bool:
    """
    Запрашивает у ОрВД разрешение на вылет.
    Дрон не должен взлетать без этого разрешения.
    """
    response = kafka_request(
        topic=ORVD_TOPIC,
        message={
            "action": "request_takeoff",
            "payload": {
                "drone_id": drone["serial_number"],
                "mission_id": mission_id,
                "time": datetime.now(timezone.utc).isoformat(),
            },
        },
        timeout=10.0,
    )
    status = (response or {}).get("payload", {}).get("status", "")
    if response is None:
        log_event("orvd_takeoff_skipped", f"ОрВД недоступен, авторизация вылета пропущена.")
        return True  # продолжаем без ОрВД
    if status == "takeoff_authorized":
        log_event("orvd_takeoff_authorized", f"ОрВД авторизовал вылет дрона {drone['serial_number']} по миссии {mission_id}.")
        return True
    reason = (response or {}).get("payload", {}).get("reason", "unknown")
    log_event("orvd_takeoff_denied", f"ОрВД отказал в вылете дрона {drone['serial_number']}: {reason}")
    return False


# ── Страховая ─────────────────────────────────────────────────────────────

def purchase_insurance(order: dict, drone: dict) -> Optional[str]:
    """Рассчитывает и покупает страховой полис. Возвращает policy_id или None."""
    request_id = f"ins-{uuid.uuid4().hex[:12]}"
    coverage = float(order.get("offered_price") or 5_000_000)

    base = {
        "request_id": request_id,
        "order_id": str(order.get("external_request_id") or order["id"]),
        "manufacturer_id": "m2_manufacturer",
        "operator_id": M2_OPERATOR_ID,
        "drone_id": drone["serial_number"],
        "security_goals": _security_goals(order),
        "coverage_amount": coverage,
        "calculation_id": None,
        "incident": None,
    }

    # CALCULATION
    calc_resp = kafka_request(
        topic=INSURER_REQUESTS_TOPIC,
        message={**base, "request_type": "CALCULATION"},
        reply_topic=INSURER_RESPONSES_TOPIC,
        timeout=8.0,
    )
    if calc_resp is None:
        log_event("insurance_skipped", "Страховая недоступна, полис не оформлен.")
        return None

    calc_payload = calc_resp.get("payload", calc_resp)
    if calc_payload.get("status") != "SUCCESS":
        log_event("insurance_calc_failed", f"Страховая отказала в расчёте: {calc_payload.get('message')}")
        return None

    log_event("insurance_calculated", f"Страховая рассчитала премию: {calc_payload.get('calculated_cost')} руб.")

    # PURCHASE
    purchase_resp = kafka_request(
        topic=INSURER_REQUESTS_TOPIC,
        message={**base, "calculation_id": calc_payload.get("response_id", request_id), "request_type": "PURCHASE"},
        reply_topic=INSURER_RESPONSES_TOPIC,
        timeout=8.0,
    )
    if purchase_resp is None:
        log_event("insurance_purchase_failed", "Страховая не ответила на покупку полиса.")
        return None

    purchase_payload = purchase_resp.get("payload", purchase_resp)
    policy_id = purchase_payload.get("policy_id")
    if policy_id:
        log_event("insurance_purchased", f"Куплен страховой полис: {policy_id}")
        return policy_id

    log_event("insurance_purchase_failed", f"Страховая не выдала полис: {purchase_payload.get('message')}")
    return None


def report_incident_to_insurer(order: dict, drone: dict, reason: str) -> None:
    """
    Отправляет страховой случай в страховую компанию при сбое или отмене миссии.
    """
    policy_id = order.get("insurance_policy_id")
    request_id = f"inc-{uuid.uuid4().hex[:12]}"

    kafka_request(
        topic=INSURER_REQUESTS_TOPIC,
        message={
            "request_id": request_id,
            "order_id": str(order.get("external_request_id") or order["id"]),
            "manufacturer_id": "m2_manufacturer",
            "operator_id": M2_OPERATOR_ID,
            "drone_id": drone["serial_number"] if drone else "unknown",
            "security_goals": _security_goals(order),
            "coverage_amount": float(order.get("offered_price") or 0),
            "calculation_id": None,
            "incident": {
                "policy_id": policy_id,
                "reason": reason,
                "time": datetime.now(timezone.utc).isoformat(),
            },
            "request_type": "INCIDENT",
        },
        reply_topic=INSURER_RESPONSES_TOPIC,
        timeout=8.0,
    )
    log_event("insurance_incident_reported", f"Страховой случай по заказу ID={order['id']}: {reason}")


# ── НУС / GCS ─────────────────────────────────────────────────────────────

def submit_mission_to_gcs(order: dict, drone: dict, mission_id: str) -> Optional[str]:
    """task.submit → task.assign в НУС."""
    waypoints = [
        {"lat": p["lat"], "lon": p["lon"], "alt": 100}
        for p in _build_route(order)
    ]

    submit_resp = kafka_request(
        topic=GCS_TOPIC,
        message={
            "action": "task.submit",
            "payload": {"task": {"waypoints": waypoints}, "mission_id": mission_id},
        },
        timeout=8.0,
    )
    if submit_resp is None:
        log_event("gcs_skipped", f"НУС недоступен, миссия {mission_id} не отправлена.")
        return None

    if not submit_resp.get("success", False):
        log_event("gcs_submit_failed", f"НУС отказал в создании задачи: {submit_resp.get('payload')}")
        return None

    gcs_mission_id = submit_resp.get("payload", {}).get("mission_id", mission_id)
    log_event("gcs_mission_submitted", f"Миссия {gcs_mission_id} принята НУС.")

    assign_resp = kafka_request(
        topic=GCS_TOPIC,
        message={
            "action": "task.assign",
            "payload": {"mission_id": gcs_mission_id, "drone_id": drone["serial_number"]},
        },
        timeout=8.0,
    )
    if assign_resp and assign_resp.get("success"):
        log_event("gcs_mission_assigned", f"Дрон {drone['serial_number']} назначен на задачу {gcs_mission_id} в НУС.")
    else:
        log_event("gcs_assign_failed", f"НУС не назначил дрон на задачу {gcs_mission_id}.")

    return gcs_mission_id


# ── Главная функция ────────────────────────────────────────────────────────

def run_mission_assignment(order: dict, drone: dict) -> dict:
    """
    Полный цикл по инструкции преподавателя:
    ОрВД (регистрация) → Страховая → НУС → ОрВД (авторизация вылета)
    """
    mission_id = f"MSN-M2-{uuid.uuid4().hex[:8].upper()}"
    result = {
        "mission_id": mission_id,
        "orvd_ok": False,
        "insurance_policy_id": None,
        "gcs_task_id": None,
    }

    log_event("mission_started", f"Миссия {mission_id} для заказа ID={order['id']} — начало.")

    # 1. ОрВД: регистрация дрона
    register_drone_in_orvd(drone)

    # 2. ОрВД: регистрация миссии
    orvd_ok = register_mission_in_orvd(order, drone, mission_id)
    result["orvd_ok"] = orvd_ok

    # 3. Страховая: расчёт + покупка полиса
    policy_id = purchase_insurance(order, drone)
    result["insurance_policy_id"] = policy_id

    # 4. НУС: отправка миссии
    gcs_task_id = submit_mission_to_gcs(order, drone, mission_id)
    result["gcs_task_id"] = gcs_task_id

    # 5. ОрВД: авторизация вылета
    takeoff_ok = request_takeoff_from_orvd(drone, mission_id)
    if not takeoff_ok:
        log_event("mission_blocked", f"Миссия {mission_id} заблокирована ОрВД — вылет запрещён.")

    log_event(
        "mission_assignment_done",
        f"Миссия {mission_id}: ОрВД={'✓' if orvd_ok else '✗'}, "
        f"страховка={policy_id or 'нет'}, НУС={gcs_task_id or 'нет'}, "
        f"вылет={'разрешён' if takeoff_ok else 'запрещён'}.",
    )
    return result
