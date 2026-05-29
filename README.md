ДЗ №7 продолжает HW6 (**Smart Warehouse**, event-driven система на Kafka + Cassandra) и поверх неё добавляет полный цикл качества: тесты всех уровней, CI/CD pipeline, нагрузочное тестирование, observability и формальные SLI/SLO.

## Что нового по сравнению с HW6

| Категория | Добавлено в HW7 |
|---|---|
| **HTTP-метрики** | Middleware `app/http_metrics.py`: `http_requests_total`, `http_request_errors_total`, `http_request_duration_seconds` для обоих сервисов |
| **Тесты** | 84 unit + integration + E2E (полный lifecycle заказа) |
| **Нагрузка** | k6 smoke-сценарий `load/smoke.js` с порогами p95 < 500ms, error rate < 1% |
| **CI/CD** | `.github/workflows/ci.yml`: lint → unit → build → integration → E2E + load + SLO check |
| **Дашборды** | Два Grafana dashboard'а: **Services** (HTTP RED) и **Infrastructure** (Kafka + Cassandra) |
| **Алерты** | 6 правил Prometheus + Alertmanager-сервис |
| **SLI/SLO** | `monitoring/slo.yml` + автоматическая проверка `scripts/check_slo.py` в CI |
| **Инфра-метрики** | `kafka-exporter` для метрик брокера и lag по consumer group'е |

## Архитектура

```
                      HTTP POST /events                Avro                              Cassandra
   Client / k6  ─────────────────────►  WMS  ─────────────►  Kafka  ─────────►  Consumer  ────────►  inventory_by_product_zone
                  (8080, /metrics)    (FastAPI)  topic:        │     (offset commit          orders_by_id, processed_events,
                                                 warehouse-    │      AFTER write)            entity_versions, ...
                                                 events        │
                                                               ▼
                                                       warehouse-events-dlq
                                                       (невалидные события)

                      ┌────────────── kafka-exporter (kafka_*, consumer_lag) ──────────────┐
                      │                                                                    ▼
   Prometheus  ◄──────┴── /metrics (WMS, consumer, lag-exporter) ◄──── Grafana ──┐  Alertmanager (9093)
   (9090)                                                                         │
       │                                                                          │
       └── alerts.yml ────────────────────────────────────────────────────────────┘
```

### Сервисы

| Сервис | Порт | Назначение |
|---|---|---|
| `wms` | 8080 | FastAPI: `POST /events`, `POST /events/bulk`, `GET /health`, `GET /metrics` |
| `consumer` | 8000 | Читает Kafka, обновляет состояние склада в Cassandra; экспортит метрики |
| `lag-exporter` | 8001 | Отдельный экспортер Kafka consumer lag (на случай падения consumer'а) |
| `kafka` + `zookeeper` + `schema-registry` | 9092, 8081 | Kafka cluster + Avro registry |
| `cassandra` | 9042 | Источник состояния склада |
| `kafka-exporter` | 9308 | Prometheus-метрики брокера и consumer group'ы |
| `prometheus` | 9090 | Сбор метрик, оценка alert rules |
| `alertmanager` | 9093 | Маршрутизация и группировка алертов |
| `grafana` | 3000 | Дашборды (`admin` / `admin`) |

## Структура проекта

```
.
├── app/                              # Общий код: avro, settings, time_utils, http_metrics
│   ├── http_metrics.py               # ★ HW7 — middleware с http_requests_total и тп
│   └── ...
├── wms_service/main.py               # FastAPI продюсер (Avro -> Kafka)
├── consumer_service/
│   ├── main.py                       # Consumer (Kafka -> Cassandra, at-least-once)
│   ├── state_processor.py            # Бизнес-логика обновления состояния (lazy import cassandra.cluster)
│   └── lag_exporter.py               # Standalone Kafka lag exporter
├── schemas/                          # Avro v1, v2
├── cassandra/init.cql                # Schema (RF=1 для CI)
├── docker-compose.yml                # ★ + alertmanager, kafka-exporter, healthchecks
├── monitoring/
│   ├── prometheus.yml                # Scrape configs + alerting block
│   ├── alerts.yml                    # ★ 6 правил: TargetDown, HighHttpErrorRate, ...
│   ├── alertmanager.yml              # ★ Маршрутизация по severity
│   ├── slo.yml                       # ★ Формальные SLO для CI
│   └── grafana/
│       ├── provisioning/             # Datasource + dashboards provisioning
│       └── dashboards/
│           ├── warehouse-services.json        # ★ HTTP RED, latency p50/p95/p99
│           └── warehouse-infrastructure.json  # ★ Kafka + Cassandra + pipeline
├── tests/                            # ★ pytest:
│   ├── unit/                         #   84 unit-tests, no external deps
│   ├── integration/                  #   producer -> Kafka -> consumer -> Cassandra
│   ├── e2e/                          #   полный order lifecycle + observability
│   └── conftest.py                   #   общие фикстуры (waiters, Cassandra session)
├── load/smoke.js                     # ★ k6 smoke test с порогами p95<500ms
├── scripts/check_slo.py              # ★ Валидатор SLO против Prometheus
├── .github/workflows/ci.yml          # ★ CI: lint → unit → build → integration → e2e+load+SLO
└── pytest.ini
```

## Метрики (HW7)

Все три обязательные HTTP-метрики экспортируются обоими сервисами через единый middleware `app/http_metrics.py`:

| Метрика | Тип | Labels |
|---|---|---|
| `http_requests_total` | Counter | `method, endpoint, status` |
| `http_request_errors_total` | Counter | `method, endpoint, error_type` |
| `http_request_duration_seconds` | Histogram | `method, endpoint` |

**Логика подсчёта ошибок:**
- 4xx — **не** считаются ошибками сервиса (это ошибки клиента), но фиксируются в `http_requests_total{status="4xx"}`.
- 5xx — увеличивают `http_request_errors_total{error_type="http_5xx"}`.
- Любое необработанное исключение — `http_request_errors_total{error_type="<ExceptionClass>"}`.

Это даёт честный показатель «доступности» сервиса для SLO и устраняет шум от валидных бизнес-отказов (например, 400 на невалидное событие).

Дополнительно consumer экспортит:
- `events_processed_total{event_type}` — счётчик обработанных событий.
- `event_processing_duration_seconds` — гистограмма длительности обработки.
- `cassandra_write_errors_total` — счётчик write-ошибок в Cassandra.

Метрику `consumer_lag{partition}` экспортит **отдельный сервис `lag-exporter`** (порт 8001), а не сам consumer — это сделано намеренно, чтобы цикл обработки сообщений не делал дорогих offset-запросов к брокеру на каждое сообщение (иначе throughput падает в ~100 раз).

И через `kafka-exporter` — `kafka_brokers`, `kafka_topic_partitions`, `kafka_consumergroup_lag`, ...

## Дашборды Grafana

### 1. Smart Warehouse — Services (HTTP RED)

Метод **RED** (Rate, Errors, Duration) для обоих сервисов:

- **Stats:** Targets up, Total RPS, Error rate (5m), p95 latency (5m)
- **Throughput** — RPS по `service` + `endpoint`
- **Latency** — p50 / p95 / p99 по сервису
- **Error rate** — доля 4xx+5xx, с порогами 1% / 5%
- **Latency heatmap** — распределение `http_request_duration_seconds_bucket`
- **HTTP errors by error_type** — bar chart `http_request_errors_total`

### 2. Smart Warehouse — Infrastructure

- **Stats:** Kafka brokers, partitions, max consumer lag, Cassandra write errors
- **Consumer lag by partition** — из `consumer_lag` и `kafka_consumergroup_lag`
- **Kafka message rate** — produce/consume rate по партициям
- **Events processed by type** — rate `events_processed_total{event_type}`
- **Event processing duration p50/p95/p99**
- **Cassandra write errors over time**

## Алерты

Описаны в `monitoring/alerts.yml`. Маршрутизируются через `alertmanager` (порт 9093):

| Alert | Severity | Условие | SLO |
|---|---|---|---|
| `TargetDown` | critical | `up == 0` 1m | availability |
| `HighHttpErrorRate` | critical | error rate > 5% за 5m | availability |
| `HighRequestLatencyP95` | warning | p95 > 1s за 5m | latency |
| `KafkaConsumerLagHigh` | warning | `max(consumer_lag) > 50` за 1m | event_processing_delay |
| `CassandraWriteErrors` | critical | `increase(cassandra_write_errors_total[1m]) > 0` | availability |
| `NoEventsProcessed` | warning | продюсер активен, но consumer не обрабатывает события 2m | event_processing_delay |

Просмотр активных алертов: <http://localhost:9090/alerts> или <http://localhost:9093>.

## SLI / SLO

Формально описаны в `monitoring/slo.yml` и автоматически валидируются в CI после нагрузочного теста через `scripts/check_slo.py`.

| SLO | Цель | SLI (PromQL) |
|---|---|---|
| **wms_availability** | ≥ 99% | `sum(rate(http_requests_total{service="wms",status!~"5.."}[5m])) / clamp_min(sum(rate(http_requests_total{service="wms"}[5m])), 1)` |
| **wms_latency_p95** | ≤ 500 ms | `histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{service="wms"}[5m])))` |
| **pipeline_freshness** | `max(consumer_lag) ≤ 50` | `max(consumer_lag)` |
| **cassandra_durability** | 0 write errors за 5m | `sum(increase(cassandra_write_errors_total[5m]))` |

При нарушении любого SLO `scripts/check_slo.py` возвращает exit code 1 и **fail'ит CI build**, печатая отчёт:

```
================================================================================
SLO                              OBJECTIVE         MEASURED       STATUS
--------------------------------------------------------------------------------
  wms_availability                       0.99            1         OK
  wms_latency_p95                         0.5         0.124         OK
  pipeline_freshness                       50            2         OK
  cassandra_durability                      0            0         OK
================================================================================
All SLOs satisfied.
```

## Тесты

### Локально

```bash
python -m venv .venv
. .venv/Scripts/activate          # PowerShell: .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

#### Unit (84 теста, без Docker, < 1s)

```bash
pytest tests/unit -v
```

Покрывают:
- `app/time_utils.py` — парсинг ISO-8601, ensure_utc, UTC roundtrip.
- `app/avro.py` — schema field filtering, version mismatch handling.
- `app/http_metrics.py` — middleware: 2xx / 4xx / 5xx / unhandled exception, label cardinality, `/metrics` endpoint.
- `wms_service.main.EventPublisher._normalize_event` — event_id/timestamp defaulting, schema version validation, nullable fields, edge cases.
- `consumer_service.state_processor` — static-method валидаторы (`_require_text`, `_require_quantity`, `_event_product_ids`, `_latest_supplier_for_product`, `consistency_level`).

#### Integration (требует docker-compose stack)

```bash
docker compose up -d --build --wait
pytest tests/integration -v
```

Покрывают:
- `POST /health`, `/metrics` экспортирует все 3 обязательные метрики.
- `POST /events` → возвращает Kafka metadata.
- Валидное событие проходит pipeline и появляется в `processed_events` и `inventory_by_product_zone`.
- Дубль `event_id` детектится и инвентарь не дублируется.
- Невалидное событие (`PRODUCT_SHIPPED` без стока) → DLQ, инвентарь не создан.
- Метрики `http_requests_total` и `events_processed_total` действительно появляются в Prometheus.

#### E2E (требует docker-compose stack)

```bash
pytest tests/e2e -v
```

Тестирует **полный жизненный цикл заказа**:

1. `PRODUCT_RECEIVED` 100 единиц в ZONE-A → `inventory.available == 100`
2. `PRODUCT_MOVED` 30 → ZONE-B → `A.available == 70, B.available == 30`
3. `ORDER_CREATED` 20 в ZONE-B → `B.available == 10, B.reserved == 20`, `orders_by_id.status = CREATED`
4. `ORDER_COMPLETED` → `B.reserved == 0`, `orders_by_id.status = COMPLETED`

Плюс observability-инварианты:
- Все Prometheus targets `up`.
- Все 6 alert rules загружены.
- `kafka-exporter` экспортит `kafka_topic_partitions{topic="warehouse-events"}`.
- Alertmanager отвечает на `/api/v2/status`.
- В Prometheus присутствуют все 3 HW7-required метрики.

### Load test (k6)

```bash
# С запущенным docker-compose:
k6 run load/smoke.js -e WMS_URL=http://localhost:8080 -e LOAD_DURATION=45s
```

10 VUs пушат смесь событий (50% RECEIVED / 30% RESERVED / 20% RELEASED) на `POST /events`. Тест **fail**'ится при:
- `http_req_duration p(95)` > 500 ms
- `http_req_failed` rate > 1%
- `success_latency p(99)` > 1000 ms

После load test'а CI запускает `scripts/check_slo.py` против Prometheus для финальной валидации.

## CI/CD Pipeline

Файл: `.github/workflows/ci.yml`. Запускается на `push`/`pull_request` в `main`.

```
┌─────┐
│lint │  ruff + python -m compileall
└──┬──┘
   ├────────────┐
   ▼            ▼
┌──────────┐  ┌───────┐
│unit-tests│  │ build │   docker build
└────┬─────┘  └───┬───┘
     │            │
     └─────┬──────┘
           ▼
   ┌───────────────────┐
   │integration-tests  │   docker compose up + pytest tests/integration
   └─────────┬─────────┘
             ▼
   ┌─────────────────────────────┐
   │e2e-and-load                 │   docker compose up
   │  ▸ pytest tests/e2e         │
   │  ▸ k6 run load/smoke.js     │
   │  ▸ check_slo.py             │   ← FAIL build if SLO breached
   │  ▸ verify http_* metrics    │   ← FAIL build if 0 series
   └─────────────────────────────┘
```

Артефакты, которые поднимаются на каждый run:
- `unit-tests-junit/unit-tests.xml`
- `integration-tests-junit/integration-tests.xml`
- `e2e-and-load-artifacts/` (e2e junit, k6 summary, k6 raw export)

## Mapping на критерии HW7

| Критерий (10 баллов) | Где реализовано |
|---|---|
| **1. Минимум 2 сервиса + БД** | `wms` (FastAPI producer) + `consumer` (Kafka -> Cassandra) + Cassandra |
| **2. CI/CD pipeline** | `.github/workflows/ci.yml`, 5 stages |
| **3. Юнит-тесты** | `tests/unit/`, 84 теста, < 1s wallclock |
| **4. Интеграционные тесты** | `tests/integration/test_pipeline.py` |
| **5. E2E тесты** | `tests/e2e/test_order_lifecycle.py` (полный order lifecycle) |
| **6. Метрики `http_requests_total`, `http_request_errors_total`, `http_request_duration_seconds`** | `app/http_metrics.py` + `install_http_metrics()` в обоих сервисах |
| **7. Dashboard для сервисов** | `monitoring/grafana/dashboards/warehouse-services.json` — 9 панелей с RED-метриками |
| **8. Dashboard для инфраструктуры** | `monitoring/grafana/dashboards/warehouse-infrastructure.json` — 9 панелей (Kafka + Cassandra + pipeline) |
| **9. Нагрузочное тестирование** | `load/smoke.js` (k6), интегрировано в CI с порогами p95<500ms |
| **10. Сценарий E2E + load + проверка метрик в CI** | Job `e2e-and-load` в `ci.yml` запускает всё последовательно и валидирует SLO |
| **Алертинг** | `monitoring/alerts.yml` (6 правил) + `alertmanager` сервис |
| **SLI/SLO** | `monitoring/slo.yml` + `scripts/check_slo.py` в CI |

## Решения и компромиссы

- **Cassandra single-node (RF=1)** вместо 3-узлового кластера из HW6 — раннеры GitHub Actions имеют 7GB RAM, и 3-узловой Cassandra с `start_period: 120s` x 3 сильно замедлил бы CI. HW7 фокусируется на CI/CD/observability, а не на resilience Cassandra.
- **Ленивый импорт `cassandra.cluster`** в `state_processor.py` — `cassandra-driver` 3.29 пытается импортировать `asyncore`, который удалён в Python 3.12. Lazy-import позволяет юнит-тестам валидаторов работать на любой машине без установленного libev.
- **Error rate считается только по 5xx**, не по 4xx — 4xx это валидные клиентские ошибки (например, `event_type is required`), которые не должны жечь бюджет ошибок сервиса.
- **kafka-exporter** вместо JMX-exporter'а — проще, не требует sidecar'а к каждой Kafka-ноде, даёт всё нужное для дашборда (lag, topic metadata, broker count).
- **Alertmanager** настроен на webhook к самому себе — в проде вместо этого был бы Slack/PagerDuty/Telegram. Цель этого compose'а — показать, что алерты доходят до Alertmanager'а и группируются по severity.
