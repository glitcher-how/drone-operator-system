from flask import Blueprint, render_template

from ..db import query_all

bp = Blueprint("events", __name__)


@bp.route("/events")
def events_page():
    events = query_all("SELECT * FROM events ORDER BY id DESC LIMIT 100")
    return render_template("events.html", events=events)