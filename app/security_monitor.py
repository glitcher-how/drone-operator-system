from dataclasses import dataclass
from datetime import date


@dataclass
class SecurityCheckResult:
    ok: bool
    errors: list[str]


def check_certificate(drone) -> str | None:
    if drone["certificate_valid_until"] < date.today().isoformat():
        return "Сертификат дрона просрочен."
    return None


def check_battery(drone, min_battery: int = 30) -> str | None:
    if int(drone["battery_level"]) < min_battery:
        return f"Недостаточный заряд батареи: {drone['battery_level']}%."
    return None


def check_payload(order, drone) -> str | None:
    if float(drone["payload_capacity"]) < float(order["cargo_weight"]):
        return f"Недостаточная грузоподъёмность: {drone['payload_capacity']} кг < {order['cargo_weight']} кг."
    return None


def check_drone_status(drone) -> str | None:
    if drone["status"] != "ready":
        return f"Дрон не готов к миссии. Статус: {drone['status']}."
    return None


def check_droneport(drone, droneport) -> str | None:
    if drone["droneport_id"] is None:
        return "Дрон не привязан к дронопорту."
    if not droneport:
        return "Дронопорт не найден."
    if droneport["status"] != "active":
        return "Дронопорт не активен."
    return None


def run_security_checks(order, drone, droneport) -> SecurityCheckResult:
    errors = []

    for result in (
        check_certificate(drone),
        check_battery(drone),
        check_payload(order, drone),
        check_drone_status(drone),
        check_droneport(drone, droneport),
    ):
        if result:
            errors.append(result)

    return SecurityCheckResult(ok=not errors, errors=errors)
