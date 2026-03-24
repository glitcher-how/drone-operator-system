import sqlite3
from flask import Blueprint, flash, redirect, render_template, request, url_for

from ..db import execute, query_all
from ..logger import publish_droneport
from ..services import droneport_load, log_event

bp = Blueprint("droneports", __name__)


@bp.route("/droneports", methods=["GET", "POST"])
def droneports_page():
    if request.method == "POST":
        try:
            execute(
                "INSERT INTO droneports(name, location, capacity, status) VALUES (?, ?, ?, ?)",
                (
                    request.form["name"].strip(),
                    request.form["location"].strip(),
                    int(request.form["capacity"]),
                    request.form["status"],
                ),
            )
            log_event(
                "droneport_created", f"Создан дронопорт '{request.form['name']}'."
            )
            publish_droneport({
                "name": request.form["name"].strip(),
                "location": request.form["location"].strip(),
                "capacity": int(request.form["capacity"]),
                "status": request.form["status"],
                "team": "M2",
            })
            flash("Дронопорт создан.")
        except sqlite3.IntegrityError:
            flash("Дронопорт с таким названием уже существует.")
        return redirect(url_for("droneports.droneports_page"))

    ports = query_all("SELECT * FROM droneports ORDER BY id DESC")
    loads = {p["id"]: droneport_load(int(p["id"])) for p in ports}
    return render_template("droneports.html", ports=ports, loads=loads)
