from dataclasses import dataclass


@dataclass
class Drone:
    id: int
    name: str
    drone_type: str
    serial_number: str
    status: str
    payload_capacity: float
    range_km: float
    battery_level: int
    certificate_valid_until: str
    droneport_id: int | None


@dataclass
class Droneport:
    id: int
    name: str
    location: str
    capacity: int
    status: str


@dataclass
class Order:
    id: int
    customer_name: str
    mission_type: str
    cargo_weight: float
    departure_point: str
    destination_point: str
    required_time: str
    status: str
    assigned_drone_id: int | None
    created_at: str


@dataclass
class Event:
    id: int
    created_at: str
    event_type: str
    details: str
