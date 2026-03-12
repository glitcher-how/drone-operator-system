import sqlite3
from flask import current_app, g


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS droneports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    location TEXT NOT NULL,
    capacity INTEGER NOT NULL CHECK(capacity >= 0),
    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active', 'inactive'))
);

CREATE TABLE IF NOT EXISTS drones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    drone_type TEXT NOT NULL,
    serial_number TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK(status IN ('new', 'registered', 'ready', 'busy', 'maintenance')),
    payload_capacity REAL NOT NULL CHECK(payload_capacity >= 0),
    range_km REAL NOT NULL CHECK(range_km >= 0),
    battery_level INTEGER NOT NULL CHECK(battery_level BETWEEN 0 AND 100),
    certificate_valid_until TEXT NOT NULL,
    droneport_id INTEGER,
    FOREIGN KEY (droneport_id) REFERENCES droneports(id)
);

CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_name TEXT NOT NULL,
    mission_type TEXT NOT NULL,
    cargo_weight REAL NOT NULL CHECK(cargo_weight >= 0),
    departure_point TEXT NOT NULL,
    destination_point TEXT NOT NULL,
    required_time TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new' CHECK(status IN ('new', 'assigned', 'in_progress', 'done', 'cancelled')),
    assigned_drone_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY (assigned_drone_id) REFERENCES drones(id)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    event_type TEXT NOT NULL,
    details TEXT NOT NULL
);
"""


def get_db() -> sqlite3.Connection:
    if "db" not in g:
        g.db = sqlite3.connect(current_app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None) -> None:
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db() -> None:
    db = get_db()
    db.executescript(SCHEMA_SQL)
    db.commit()


def query_all(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    return get_db().execute(sql, params).fetchall()


def query_one(sql: str, params: tuple = ()):
    return get_db().execute(sql, params).fetchone()


def execute(sql: str, params: tuple = ()) -> None:
    db = get_db()
    db.execute(sql, params)
    db.commit()