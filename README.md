# LedgeFlow

Event-sourced financial ledger implementing CQRS with Kafka Streams. Account state is
derived entirely from an immutable Kafka event log — PostgreSQL serves as a rebuildable
read model, never the source of truth.

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
- JWT authentication — register, login, role-based access
- Kafka producer — all financial operations publish typed events to `account.events`
- Event consumer — reads Kafka, updates PostgreSQL read model with idempotency
- Kafka Streams topology — KTable balance aggregation, windowed velocity detection, anomaly routing to `account.alerts`
- Admin rebuild endpoint — deletes the entire read model and replays Kafka from offset 0
- Micrometer metrics exposed at `/actuator/prometheus`
- OpenTelemetry tracing — trace sampling at 100%, propagated through Kafka headers
- Testcontainers integration tests — deposit and withdrawal flows verified end-to-end against real Kafka and PostgreSQL
- Flyway versioned migrations — four tables managed

## Status

Work in progress — Docker Compose, Grafana dashboard, and remaining integration tests coming next.
