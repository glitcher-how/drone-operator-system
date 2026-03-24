from datetime import datetime

from flask import Blueprint, flash, redirect, render_template, request, url_for

from ..db import execute, query_all, query_one
from ..logger import log_event, publish_order_result, publish_price_offer
from ..mission_orchestrator import run_mission_assignment
from ..services import select_best_drone

bp = Blueprint("orders", __name__)


@bp.route("/orders", methods=["GET", "POST"])
def orders_page():
    if request.method == "POST":
        execute(
            """
            INSERT INTO orders(
                customer_name, mission_type, cargo_weight, departure_point,
                destination_point, required_time, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'new', ?)
            """,
            (
                request.form["customer_name"].strip(),
                request.form["mission_type"].strip(),
                float(request.form["cargo_weight"]),
                request.form["departure_point"].strip(),
                request.form["destination_point"].strip(),
                request.form["required_time"],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ),
        )
        log_event(
            "order_created",
            f"Создан заказ-наряд для клиента '{request.form['customer_name']}'.",
        )
        flash("Заказ-наряд создан.")
        return redirect(url_for("orders.orders_page"))

    orders = query_all("""
        SELECT o.*, d.name AS drone_name, d.serial_number
        FROM orders o
        LEFT JOIN drones d ON d.id = o.assigned_drone_id
        ORDER BY o.id DESC
        """)
    return render_template("orders.html", orders=orders)


@bp.post("/orders/<int:order_id>/assign")
def assign_order_drone(order_id: int):
    order = query_one("SELECT * FROM orders WHERE id = ?", (order_id,))
    if not order:
        flash("Заказ не найден.")
        return redirect(url_for("orders.orders_page"))

    drone, errors = select_best_drone(order_id)
    if not drone:
        flash("Подходящий дрон не найден.")
        for err in errors[:5]:
            flash(err)
        return redirect(url_for("orders.orders_page"))

    execute(
        "UPDATE orders SET assigned_drone_id = ?, status = 'assigned' WHERE id = ?",
        (drone["id"], order_id),
    )
    execute("UPDATE drones SET status = 'busy' WHERE id = ?", (drone["id"],))
    log_event(
        "drone_selected_for_order",
        f"Для заказа ID={order_id} выбран дрон ID={drone['id']} ({drone['name']}).",
    )

    # Полный цикл миссии: ОрВД → Страховая → НУС
    fresh_order = query_one("SELECT * FROM orders WHERE id = ?", (order_id,))
    mission_result = run_mission_assignment(dict(fresh_order), dict(drone))

    execute(
        """UPDATE orders SET
            mission_id = ?, insurance_policy_id = ?, gcs_task_id = ?, orvd_ok = ?
           WHERE id = ?""",
        (
            mission_result["mission_id"],
            mission_result["insurance_policy_id"],
            mission_result["gcs_task_id"],
            1 if mission_result["orvd_ok"] else 0,
            order_id,
        ),
    )

    # Если заказ пришёл от Aggregator — отправляем price_offer
    if fresh_order["source"] == "aggregator" and fresh_order["external_request_id"]:
        price = float(fresh_order["offered_price"] or 2500.0)
        publish_price_offer(
            request_id=fresh_order["external_request_id"],
            order_id=order_id,
            price=price,
        )
        log_event("price_offer_sent", f"Отправлен price_offer Aggregator'у по заказу ID={order_id}.")

    flash(
        f"Дрон {drone['name']} назначен. "
        f"Миссия: {mission_result['mission_id']} | "
        f"Страховка: {mission_result['insurance_policy_id'] or 'нет'} | "
        f"НУС: {mission_result['gcs_task_id'] or 'нет'}."
    )
    return redirect(url_for("orders.orders_page"))


@bp.post("/orders/<int:order_id>/complete")
def complete_order(order_id: int):
    order = query_one("SELECT * FROM orders WHERE id = ?", (order_id,))
    if not order:
        flash("Заказ не найден.")
        return redirect(url_for("orders.orders_page"))

    if order["status"] != "assigned":
        flash("Можно завершить только назначенный заказ.")
        return redirect(url_for("orders.orders_page"))

    # завершить заказ
    execute("UPDATE orders SET status = 'done' WHERE id = ?", (order_id,))

    # освободить дрон
    execute(
        "UPDATE drones SET status = 'ready' WHERE id = ?", (order["assigned_drone_id"],)
    )

    log_event(
        "mission_completed",
        f"Заказ ID={order_id} завершён. Дрон ID={order['assigned_drone_id']} снова готов.",
    )

    # Если заказ от Aggregator — отправляем order_result
    if order["source"] == "aggregator" and order["external_request_id"]:
        price = float(order["offered_price"] or 0.0)
        publish_order_result(
            request_id=order["external_request_id"],
            success=True,
            total_price=price,
        )
        log_event("order_result_sent", f"Отправлен order_result Aggregator'у по заказу ID={order_id}.")

    flash("Миссия завершена.")
    return redirect(url_for("orders.orders_page"))
