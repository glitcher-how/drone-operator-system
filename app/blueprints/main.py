from flask import Blueprint, render_template

from ..db import query_one

bp = Blueprint("main", __name__)


@bp.route("/")
def index():
    stats = {
        "drones": query_one("SELECT COUNT(*) AS c FROM drones")["c"],
        "droneports": query_one("SELECT COUNT(*) AS c FROM droneports")["c"],
        "orders": query_one("SELECT COUNT(*) AS c FROM orders")["c"],
        "events": query_one("SELECT COUNT(*) AS c FROM events")["c"],
    }
    return render_template("index.html", stats=stats)