# Система управления дронами — Команда М2

Учебный проект курса **ПИКС МК (Методология кибериммунитета)**.
Роль в архитектуре: **Эксплуатант БВС (оператор дронов)**.

Система автоматизирует полный жизненный цикл миссий беспилотных летательных аппаратов — от приёма заказа до завершения полёта — с интеграцией в общую Kafka-инфраструктуру курса.

---

## Архитектура системы

М2 взаимодействует со следующими участниками через Apache Kafka:

| Участник | Роль |
|---|---|
| **Агрегатор** | Присылает заказы, подтверждает цены |
| **ОрВД** | Регистрирует дроны и миссии, выдаёт разрешение на вылет |
| **Страховая** | Рассчитывает и выдаёт страховой полис |
| **НУС (GCS)** | Получает задачи и назначает дроны |
| **DronePort** | Предоставляет список доступных дронов |
| **Регулятор** | Регистрирует топики системы при старте |

### Полный цикл миссии

```
Агрегатор → create_order
    ↓
Оператор назначает дрон
    ↓
ОрВД: register_drone → register_mission
    ↓
Страховая: CALCULATION → PURCHASE (полис)
    ↓
НУС: task.submit → task.assign
    ↓
ОрВД: request_takeoff (разрешение на вылет)
    ↓
Агрегатор ← price_offer
    ↓
[Агрегатор: confirm_price]
    ↓
Оператор завершает/отменяет миссию
    ↓
Страховая: INCIDENT (только при отмене с полисом)
Агрегатор ← order_result
```

### Жизненный цикл заказа

```
new → assigned → done
           ↘ cancelled
```

---

## Структура проекта

```
mk2/
├── app.py                      # Точка входа (Flask)
├── config.py                   # Конфигурация
├── requirements.txt
├── .env.example                # Шаблон переменных окружения
└── app/
    ├── __init__.py             # Создание Flask-приложения, запуск Kafka consumer
    ├── db.py                   # SQLite: схема, миграции, хелперы
    ├── seeds.py                # Начальные данные (дроны и дронопорты М2)
    ├── logger.py               # log_event(), publish_price_offer(), publish_order_result()
    ├── services.py             # Выбор лучшего дрона (security monitor)
    ├── kafka_consumer.py       # Подписка на Aggregator, DronePort, НУС (телеметрия)
    ├── kafka_rpc.py            # Синхронный запрос-ответ через Kafka (correlation_id)
    ├── kafka_client.py         # KafkaProducer с SASL-аутентификацией
    ├── mission_orchestrator.py # Полный цикл: ОрВД, Страховая, НУС
    ├── blueprints/
    │   ├── orders.py           # Маршруты: создание, назначение, завершение, отмена
    │   ├── drones.py           # CRUD дронов
    │   ├── droneports.py       # CRUD дронопортов
    │   ├── events.py           # Журнал событий
    │   └── main.py             # Главная страница
    └── templates/
        ├── base.html
        ├── index.html
        ├── orders.html
        ├── drones.html
        ├── droneports.html
        └── events.html
```

---

## Установка и запуск

### 1. Установить зависимости

```bash
pip install -r requirements.txt
```

### 2. Настроить переменные окружения

Скопировать `.env.example` в `.env` и заполнить:

```bash
cp .env.example .env
```

Обязательные параметры:

```env
KAFKA_BOOTSTRAP_SERVERS=<адрес брокера>
BROKER_USER=admin
BROKER_PASSWORD=admin_secret_123
SECRET_KEY=<любая случайная строка>
```

### 3. Запустить приложение

```bash
python app.py
```

Веб-интерфейс доступен по адресу: **http://127.0.0.1:5000**

При наличии `KAFKA_BOOTSTRAP_SERVERS`:
- автоматически запускается Kafka consumer в фоновом потоке
- система регистрирует свои топики у Регулятора
- начинается периодическая синхронизация дронов из DronePort (раз в 60 сек)

---

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | — | Адрес Kafka брокера (обязательно для интеграции) |
| `BROKER_USER` | — | SASL логин |
| `BROKER_PASSWORD` | — | SASL пароль |
| `SECRET_KEY` | `dev` | Flask secret key |
| `DATABASE` | `instance/mk2.db` | Путь к БД SQLite |
| `AGGREGATOR_REQUESTS_TOPIC` | `v1.aggregator_insurer.local.operator.requests` | Входящие заказы от Агрегатора |
| `AGGREGATOR_RESPONSES_TOPIC` | `v1.aggregator_insurer.local.operator.responses` | Ответы Агрегатору |
| `ORVD_TOPIC` | `v1.ORVD.ORVD001.main` | Топик ОрВД |
| `INSURER_REQUESTS_TOPIC` | `v1.Insurer.1.insurer-service.requests` | Запросы в страховую |
| `INSURER_RESPONSES_TOPIC` | `v1.Insurer.1.insurer-service.responses` | Ответы страховой |
| `GCS_TOPIC` | `v1.gcs.1.orchestrator` | Управляющий топик НУС |
| `GCS_TELEMETRY_TOPIC` | `v1.gcs.1.drone_manager` | Телеметрия от НУС |
| `DRONEPORT_REGISTRY_TOPIC` | `v1.drone_port.1.registry` | Реестр DronePort |
| `M2_REPLY_TOPIC` | `v1.replies.m2_operator.droneport` | Ответы DronePort для М2 |
| `REGULATOR_TOPIC` | `v1.regulator.1.registration` | Регистрация у Регулятора |
| `DRONEPORT_SYNC_INTERVAL` | `60` | Интервал синхронизации дронов (сек) |

---

## Функциональность веб-интерфейса

- **Дроны** — регистрация, просмотр статуса, уровня заряда, сертификата
- **Дронопорты** — управление базами дронов
- **Заказы** — создание вручную, назначение дрона, завершение и отмена миссии
- **Журнал событий** — полная история всех действий системы

### Безопасность при выборе дрона (Security Monitor)

Дрон допускается к миссии только при выполнении всех условий:
- статус `ready`
- заряд батареи ≥ 20%
- грузоподъёмность ≥ вес груза
- сертификат действителен
- дронопорт активен

---

## Интеграция с Kafka

### Входящие сообщения

| Топик | Тип | Действие |
|---|---|---|
| Aggregator requests | `create_order` | Создать заказ в БД |
| Aggregator requests | `confirm_price` | Перевести заказ в `in_progress` |
| DronePort reply | `success: true` | Синхронизировать список дронов |
| НУС telemetry | `telemetry.save` | Обновить заряд батареи дрона |

### Исходящие сообщения

| Получатель | Событие | Когда |
|---|---|---|
| ОрВД | `register_drone` | При назначении дрона |
| ОрВД | `register_mission` | При назначении дрона |
| ОрВД | `request_takeoff` | После отправки в НУС |
| Страховая | `CALCULATION` | При назначении дрона |
| Страховая | `PURCHASE` | После расчёта премии |
| Страховая | `INCIDENT` | При отмене миссии с полисом |
| НУС | `task.submit` | При назначении дрона |
| НУС | `task.assign` | После submit |
| Агрегатор | `price_offer` | После полного цикла ОрВД/страховой/НУС |
| Агрегатор | `order_result` | При завершении или отмене миссии |
| Регулятор | `register_system` | При старте системы |

---

## Авторы

Курс: **ПИКС МК — Методология кибериммунитета**

- Наумова Дана
- Чеботарева Вероника
- Ковалев Александр
