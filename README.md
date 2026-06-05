# LedgeFlow

Event-sourced financial ledger implementing CQRS with Kafka Streams. Account state is
derived entirely from an immutable Kafka event log — PostgreSQL serves as a rebuildable
read model, never the source of truth.

## Stack

- Java 21 · Spring Boot 4
- Apache Kafka · Kafka Streams · Confluent Schema Registry · Avro
- PostgreSQL 16 · Flyway
- Spring Security · JWT
- OpenTelemetry · Micrometer · Prometheus · Grafana
- Testcontainers · Docker Compose

## Status

Work in progress.
