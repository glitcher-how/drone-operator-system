import logging
import os

from flask import Flask

from config import Config
from .db import init_db
from .blueprints.main import bp as main_bp
from .blueprints.drones import bp as drones_bp
from .blueprints.droneports import bp as droneports_bp
from .blueprints.orders import bp as orders_bp
from .blueprints.events import bp as events_bp

logging.basicConfig(level=logging.INFO)


def create_app() -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_object(Config)

    with app.app_context():
        init_db()
        from .seeds import seed_data

        seed_data()

    app.register_blueprint(main_bp)
    app.register_blueprint(drones_bp)
    app.register_blueprint(droneports_bp)
    app.register_blueprint(orders_bp)
    app.register_blueprint(events_bp)

    # Запускаем Kafka consumer если задан адрес брокера
    kafka_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if kafka_servers:
        from .kafka_consumer import start_consumer
        start_consumer(kafka_servers, app)
    else:
        logging.getLogger(__name__).warning(
            "KAFKA_BOOTSTRAP_SERVERS не задан — Kafka consumer не запущен. "
            "Дроны других команд не будут синхронизироваться."
        )

    return app
