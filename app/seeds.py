from datetime import date, timedelta

from .db import execute, query_one
from .services import log_event


def seed_data() -> None:
    has_ports = query_one("SELECT COUNT(*) AS c FROM droneports")
    if has_ports and has_ports["c"] > 0:
        return

    execute(
        "INSERT INTO droneports(name, location, capacity, status) VALUES (?, ?, ?, ?)",
        ("PORT-01", "Казань", 3, "active"),
    )
    execute(
        "INSERT INTO droneports(name, location, capacity, status) VALUES (?, ?, ?, ?)",
        ("PORT-02", "Иннополис", 2, "active"),
    )

    valid_date = (date.today() + timedelta(days=180)).isoformat()

    execute(
        """
        INSERT INTO drones(name, drone_type, serial_number, status, payload_capacity, range_km, battery_level, certificate_valid_until, droneport_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("AgroDrone A1", "agro", "SN-A1001", "ready", 10.0, 30.0, 80, valid_date, 1),
    )
    execute(
        """
        INSERT INTO drones(name, drone_type, serial_number, status, payload_capacity, range_km, battery_level, certificate_valid_until, droneport_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("InspectDrone I1", "inspection", "SN-I1001", "ready", 3.0, 20.0, 65, valid_date, 2),
    )

    log_event("seed_loaded", "Тестовые данные загружены.")