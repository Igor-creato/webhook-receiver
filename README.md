# Webhook Receiver — Микросервис приёма постбэков от CPA-сетей

Принимает постбэки (webhooks) от CPA-сетей (Admitad, ActionPay, CityAds и др.),
трансформирует через настраиваемый маппинг и записывает в базу WordPress/WooCommerce.

## Архитектура

```
CPA-сеть → GET/POST /wh/{slug}/{secret}
                │
                ▼
        ┌──────────────┐
        │   Receiver   │  (FastAPI, 4 workers, порт 8099)
        │  rate limit  │
        └──────┬───────┘
               │ LPUSH
               ▼
        ┌──────────────┐
        │    Redis     │  (очередь + статистика)
        └──────┬───────┘
               │ BRPOP
               ▼
        ┌──────────────┐
        │   Worker     │  (4 потока)
        │  маппинг     │  → cashback_webhooks (дедупликация)
        │  валидация   │  → cashback_transactions
        │  запись в БД │  → cashback_unregistered_transactions
        └──────────────┘

        ┌──────────────┐
        │  Admin UI    │  (порт 8098, только localhost)
        │  SSH tunnel  │
        └──────────────┘
```

## Быстрый старт

### Автоматическая установка (рекомендуется)

```bash
git clone <repo> webhook-receiver
cd webhook-receiver
chmod +x install.sh
./install.sh
```

Скрипт:
1. Проверит наличие `docker`, `docker compose`, сетей `proxy` и `db-shared`.
2. Спросит домен для вебхуков и параметры подключения к БД WordPress.
3. Сгенерирует пароль админки и запишет в `.env` (chmod 600).
4. Соберёт образ, инициализирует `config.json` (DB-настройки), поднимет стек.

После установки **примените миграцию БД вручную** (один раз):
```bash
mysql -u root -p <db_name> < migration.sql
```

---

### Ручная установка

#### 1. Подготовка

```bash
git clone <repo> webhook-receiver
cd webhook-receiver
cp .env.example .env
```

**Отредактируйте `.env`:**
```
ADMIN_SECRET=ваш_надёжный_пароль
WEBHOOK_DOMAIN=webhook.example.com
```

#### 2. Миграция БД

```bash
mysql -u root -p wordpress < migration.sql
```

#### 3. Запуск

```bash
docker compose up -d
```

#### 4. Доступ к админке

Админка слушает **только на 127.0.0.1:8098**. Доступ через SSH-туннель:

```bash
ssh -L 8098:localhost:8098 user@your-server
```

Затем откройте: http://localhost:8098

### 5. Настройка

1. **База данных** → укажите хост, порт, логин, пароль MySQL вашего WordPress
2. **Сети** → импортируйте из БД или добавьте вручную
3. **Маппинг** → настройте соответствие полей CPA-сети вашей схеме
4. **URL** → скопируйте сгенерированный webhook URL и вставьте в настройки CPA-сети

## Безопасность

| Мера | Реализация |
|------|-----------|
| SQL-инъекции | Все запросы через `%s` placeholders PyMySQL |
| CSRF | Session cookies с `httponly`, `samesite=strict` |
| Brute-force | HMAC-сравнение пароля |
| Rate limiting | 200 хуков/мин на сеть, 429 при превышении |
| Доступ к админке | Только `127.0.0.1:8098` (через SSH) |
| Secret path | 24-байт `token_urlsafe` в URL вебхука |
| Дедупликация | SHA-256 от payload в `cashback_webhooks` |
| Идемпотентность | UNIQUE KEY на `idempotency_key` |
| Валидация slug | Только `[a-z0-9_-]`, макс 64 символа |
| Префикс таблиц | Regex-валидация `^[a-zA-Z0-9_]+$` |

## Производительность

- **Receiver:** 4 uvicorn workers, async Redis → ~3000+ req/s
- **Worker:** 4 потока, BRPOP → обработка ~500 msg/s
- **Redis:** буфер между приёмом и записью, сглаживает пики
- **1000 хуков/мин** = ~17/сек — запас x10 минимум

## Маппинг полей

В интерфейсе настраивается соответствие:

```
Наше поле (БД)      =    Параметр CPA-сети
────────────────────────────────────────────
click_id             =    subid1
user_id              =    subid2
uniq_id              =    admitad_id
order_number         =    order_id
offer_name           =    offer_name
order_status         =    payment_status
comission            =    payment_sum
...
```

## Логика обработки

1. Webhook приходит → сохраняется raw JSON в Redis
2. Worker достаёт из очереди
3. Записывает в `cashback_webhooks` (дедупликация по SHA-256)
4. Применяет маппинг полей
5. Маппит статус заказа (approved → completed, pending → waiting)
6. Проверяет `user_id` в `wp_users`
7. Если пользователь есть → `cashback_transactions`
8. Если нет → `cashback_unregistered_transactions`
9. Триггер MySQL автоматически считает кешбэк

## Файловая структура

```
webhook-receiver/
├── docker-compose.yml     # Оркестрация контейнеров
├── Dockerfile             # Образ Python 3.12
├── .env.example           # Пример переменных окружения
├── migration.sql          # SQL миграция для WordPress БД
├── app/
│   ├── receiver.py        # FastAPI webhook endpoint
│   ├── config.py          # JSON конфигурация
│   └── db.py              # MySQL операции (parameterized)
├── worker/
│   └── processor.py       # Redis consumer + бизнес-логика
├── admin/
│   └── panel.py           # Admin UI (FastAPI + Jinja2)
└── templates/
    ├── base.html           # Базовый layout
    ├── login.html          # Вход
    ├── dashboard.html      # Дашборд
    ├── db_settings.html    # Настройки БД
    ├── networks.html       # Список сетей
    ├── network_edit.html   # Редактор сети + маппинг
    └── logs.html           # Журнал вебхуков
```

## Мониторинг

- `/health` (порт 8099) — healthcheck для мониторинга
- Дашборд показывает размер очереди Redis и DLQ
- Dead Letter Queue сохраняет сообщения с ошибками (до 10K)

## Обновление

```bash
cd webhook-receiver
git pull
docker compose build
docker compose up -d
```

Данные сохраняются в Docker volumes (`app_data`, `redis_data`).
