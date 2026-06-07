# LedgeFlow

Event-sourced financial ledger implementing CQRS with Kafka Streams. Account state is
derived entirely from an immutable Kafka event log — PostgreSQL serves as a rebuildable
read model, never the source of truth.

## Getting started

```bash
docker compose up --build
```

| Service    | URL                          |
|------------|------------------------------|
| API        | http://localhost:8080        |
| Prometheus | http://localhost:9090        |
| Grafana    | http://localhost:3000        |

Grafana credentials: `admin` / `admin`. The LedgeFlow dashboard loads automatically.

### Quick API walkthrough

```bash
# Register and get a JWT
TOKEN=$(curl -s -X POST http://localhost:8080/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"alice","password":"secret"}')

# Create an account
ACCOUNT=$(curl -s -X POST http://localhost:8080/accounts \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ownerId":"00000000-0000-0000-0000-000000000001","currency":"EUR"}')

ID=$(echo $ACCOUNT | grep -o '"id":"[^"]*"' | cut -d'"' -f4)

# Deposit
curl -s -X POST http://localhost:8080/accounts/$ID/deposit \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"amount":100.00,"currency":"EUR"}'

# Check balance (give the event consumer a moment to process)
curl -s http://localhost:8080/accounts/$ID \
  -H "Authorization: Bearer $TOKEN"
```

## Stack

- Java 21 · Spring Boot 4
- Apache Kafka · Kafka Streams
- PostgreSQL 16 · Flyway
- Spring Security · JWT
- Micrometer · Prometheus
- OpenTelemetry
- Testcontainers · Docker Compose

## What's built so far

- REST API — accounts, deposit, withdrawal, transfer
- JWT authentication — register, login, role-based access; role stored as JWT claim, enforced by Spring Security
- Kafka producer — all financial operations publish typed events to `account.events`
- Event consumer — reads Kafka, updates PostgreSQL read model with idempotency
- Kafka Streams topology — KTable balance aggregation (transfer events fan-out to both accounts), windowed velocity detection, anomaly routing to `account.alerts`
- Admin rebuild endpoint — deletes the entire read model and replays Kafka from offset 0
- Micrometer metrics exposed at `/actuator/prometheus`
- OpenTelemetry tracing — trace sampling at 100%, propagated through Kafka headers
- Testcontainers integration tests — deposit, withdrawal, transfer, idempotency, and admin rebuild verified end-to-end against real Kafka and PostgreSQL
- Flyway versioned migrations — four tables managed
- Docker Compose — `docker compose up --build` starts the full stack: app, Kafka, PostgreSQL, Prometheus, Grafana

## Status

Work in progress — README architecture diagram and dashboard screenshot coming next.
