from datetime import date, datetime

from .db import query_all, query_one
from .droneport_client import get_available_drones
from .logger import log_event
from .security_monitor import run_security_checks


def droneport_load(droneport_id: int) -> int:
    row = query_one(
        "SELECT COUNT(*) AS cnt FROM drones WHERE droneport_id = ?",
        (droneport_id,),
    )
    return int(row["cnt"]) if row else 0


def certificate_expiring_soon(cert_date: str, days: int = 30) -> bool:
    cert = datetime.strptime(cert_date, "%Y-%m-%d").date()
    delta = cert - date.today()
    return 0 <= delta.days <= days


def select_best_drone(order_id: int):
    order = query_one("SELECT * FROM orders WHERE id = ?", (order_id,))
    if not order:
        return None, ["Заказ не найден."]

    log_event(
        "droneport_available_drones_requested",
        f"Для заказа ID={order_id} запрошен список доступных дронов из DronePort.",
    )

    available_serials = get_available_drones()

    if not available_serials:
        log_event(
            "droneport_available_drones_received",
            f"Для заказа ID={order_id} DronePort не вернул доступных дронов.",
        )
        return None, ["DronePort не вернул доступных дронов."]

    log_event(
        "droneport_available_drones_received",
        f"Для заказа ID={order_id} получены доступные дроны: {', '.join(available_serials)}.",
    )

    placeholders = ",".join("?" for _ in available_serials)

    candidates = query_all(
        f"""
        SELECT
            d.*,
            p.name AS droneport_name,
            p.location AS droneport_location,
            p.status AS droneport_status
        FROM drones d
        LEFT JOIN droneports p ON p.id = d.droneport_id
        WHERE d.serial_number IN ({placeholders})
        ORDER BY
            CASE
                WHEN LOWER(TRIM(p.location)) = LOWER(TRIM(?)) THEN 0
                ELSE 1
            END,
            d.battery_level DESC,
            d.payload_capacity ASC,
            d.id ASC
        """,
        tuple(available_serials) + (order["departure_point"],),
    )

    reasons = []

    for drone in candidates:
        droneport = None

        if drone["droneport_id"] is not None:
            droneport = query_one(
                "SELECT * FROM droneports WHERE id = ?",
                (drone["droneport_id"],),
            )

        result = run_security_checks(order, drone, droneport)

        if result.ok:
            same_location = (
                drone["droneport_location"] is not None
                and drone["droneport_location"].strip().lower()
                == order["departure_point"].strip().lower()
            )

            if same_location:
                log_event(
                    "drone_selected_nearest",
                    (
                        "Для заказа ID={order_id} выбран ближайший дрон "
                        "ID={drone_id} из локации '{location}'."
                    ).format(
                        order_id=order_id,
                        drone_id=drone["id"],
                        location=drone["droneport_location"],
                    ),
                )
            else:
                log_event(
                    "drone_selected_fallback",
                    (
                        "Для заказа ID={order_id} не найден дрон в точке отправления, "
                        "выбран резервный дрон ID={drone_id}."
                    ).format(order_id=order_id, drone_id=drone["id"]),
                )

            log_event(
                "security_check_passed",
                (
                    "Проверка безопасности пройдена для дрона ID={drone_id} "
                    "по заказу ID={order_id}."
                ).format(order_id=order_id, drone_id=drone["id"]),
            )

            return drone, []

        text = f"Дрон ID={drone['id']} ({drone['name']}): " + "; ".join(result.errors)

        reasons.append(text)

        log_event(
            "security_check_failed",
            f"Заказ ID={order_id}: {text}",
        )

    return None, reasons
