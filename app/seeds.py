from datetime import date, timedelta

from .db import execute, query_one
from .logger import log_event, publish_drone, publish_droneport

# Дронопорты и дроны команды М2.
# Дроны и дронопорты других команд приходят автоматически через Kafka consumer.
M2_DRONEPORTS = [
    {"name": "M2-PORT-01", "location": "Казань", "capacity": 3, "status": "active"},
    {"name": "M2-PORT-02", "location": "Иннополис", "capacity": 2, "status": "active"},
]

M2_DRONES = [
    {
        "name": "AgroDrone A1",
        "drone_type": "agro",
        "serial_number": "SN-A1001",
        "status": "ready",
        "payload_capacity": 10.0,
        "range_km": 30.0,
        "battery_level": 80,
        "droneport_index": 0,  # M2-PORT-01
    },
    {
        "name": "InspectDrone I1",
        "drone_type": "inspection",
        "serial_number": "SN-I1001",
        "status": "ready",
        "payload_capacity": 3.0,
        "range_km": 20.0,
        "battery_level": 65,
        "droneport_index": 1,  # M2-PORT-02
    },
]


def seed_data() -> None:
    has_ports = query_one("SELECT COUNT(*) AS c FROM droneports")
    if has_ports and has_ports["c"] > 0:
        return

    valid_date = (date.today() + timedelta(days=180)).isoformat()

    # Создаём дронопорты М2 и публикуем в Kafka
    port_ids = []
    for port in M2_DRONEPORTS:
        execute(
            "INSERT INTO droneports(name, location, capacity, status) VALUES (?, ?, ?, ?)",
            (port["name"], port["location"], port["capacity"], port["status"]),
        )
        row = query_one("SELECT id FROM droneports WHERE name = ?", (port["name"],))
        port_ids.append(row["id"])
        publish_droneport({**port, "team": "M2"})

    # Создаём дроны М2 и публикуем в Kafka
    for drone in M2_DRONES:
        droneport_id = port_ids[drone["droneport_index"]]
        execute(
            """
            INSERT INTO drones(
                name, drone_type, serial_number, status, payload_capacity,
                range_km, battery_level, certificate_valid_until, droneport_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                drone["name"],
                drone["drone_type"],
                drone["serial_number"],
                drone["status"],
                drone["payload_capacity"],
                drone["range_km"],
                drone["battery_level"],
                valid_date,
                droneport_id,
            ),
        )
        publish_drone({
            "name": drone["name"],
            "drone_type": drone["drone_type"],
            "serial_number": drone["serial_number"],
            "status": drone["status"],
            "payload_capacity": drone["payload_capacity"],
            "range_km": drone["range_km"],
            "battery_level": drone["battery_level"],
            "certificate_valid_until": valid_date,
            "team": "M2",
        })

    log_event("seed_loaded", "Тестовые данные М2 загружены и опубликованы в Kafka.")
