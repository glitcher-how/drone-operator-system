import sqlite3
from datetime import date

from flask import Blueprint, flash, redirect, render_template, request, url_for

from ..db import execute, query_all, query_one
from ..logger import publish_drone
from ..services import droneport_load, log_event

bp = Blueprint("drones", __name__)


@bp.route("/drones", methods=["GET", "POST"])
def drones_page():
    if request.method == "POST":
        cert = request.form["certificate_valid_until"]
        if cert < date.today().isoformat():
            flash("Нельзя зарегистрировать дрон с просроченным сертификатом.")
            return redirect(url_for("drones.drones_page"))

        try:
            execute(
                """
                INSERT INTO drones(
                    name, drone_type, serial_number, status, payload_capacity,
                    range_km, battery_level, certificate_valid_until
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    request.form["name"].strip(),
                    request.form["drone_type"].strip(),
                    request.form["serial_number"].strip(),
                    request.form["status"],
                    float(request.form["payload_capacity"]),
                    float(request.form["range_km"]),
                    int(request.form["battery_level"]),
                    cert,
                ),
            )
            log_event(
                "drone_registered", f"Зарегистрирован дрон '{request.form['name']}'."
            )
            publish_drone({
                "name": request.form["name"].strip(),
                "drone_type": request.form["drone_type"].strip(),
                "serial_number": request.form["serial_number"].strip(),
                "status": request.form["status"],
                "payload_capacity": float(request.form["payload_capacity"]),
                "range_km": float(request.form["range_km"]),
                "battery_level": int(request.form["battery_level"]),
                "certificate_valid_until": cert,
                "team": "M2",
            })
            flash("Дрон зарегистрирован.")
        except sqlite3.IntegrityError:
            flash("Дрон с таким серийным номером уже существует.")
        return redirect(url_for("drones.drones_page"))

    drones = query_all("""
        SELECT d.*, p.name AS droneport_name
        FROM drones d
        LEFT JOIN droneports p ON p.id = d.droneport_id
        ORDER BY d.id DESC
        """)
    ports = query_all("SELECT * FROM droneports WHERE status = 'active' ORDER BY name")
    unassigned_drones = query_all(
        "SELECT * FROM drones WHERE droneport_id IS NULL ORDER BY id DESC"
    )
    from ..services import certificate_expiring_soon

    return render_template(
        "drones.html",
        drones=drones,
        ports=ports,
        unassigned_drones=unassigned_drones,
        certificate_expiring_soon=certificate_expiring_soon,
    )


@bp.post("/assign-droneport")
def assign_droneport():
    drone_id = int(request.form["drone_id"])
    droneport_id = int(request.form["droneport_id"])

    drone = query_one("SELECT * FROM drones WHERE id = ?", (drone_id,))
    port = query_one("SELECT * FROM droneports WHERE id = ?", (droneport_id,))

    if not drone or not port:
        flash("Дрон или дронопорт не найден.")
        return redirect(url_for("drones.drones_page"))

    if drone["droneport_id"] is not None:
        flash("Этот дрон уже привязан к дронопорту.")
        return redirect(url_for("drones.drones_page"))

    if port["status"] != "active":
        flash("Дронопорт не активен.")
        return redirect(url_for("drones.drones_page"))

    if droneport_load(droneport_id) >= int(port["capacity"]):
        flash("В дронопорту нет свободных мест.")
        return redirect(url_for("drones.drones_page"))

    execute(
        """
        UPDATE drones
        SET droneport_id = ?,
            status = CASE WHEN status = 'registered' THEN 'ready' ELSE status END
        WHERE id = ?
        """,
        (droneport_id, drone_id),
    )
    log_event(
        "drone_assigned_to_port",
        f"Дрон ID={drone_id} привязан к дронопорту ID={droneport_id}.",
    )
    flash("Дрон привязан к дронопорту.")
    return redirect(url_for("drones.drones_page"))
