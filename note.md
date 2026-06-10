# Dev notes  -  Testcontainers integration test

Things I ran into and what fixed them.

---

## Spring Boot 4: @EnableKafka and kafkaListenerContainerFactory are coupled

In Spring Boot 3 these two lived in separate conditional inner classes inside
`KafkaAutoConfiguration`, so you could override one without losing the other.

In Spring Boot 4 they are in the same class (`KafkaAnnotationDrivenConfiguration`)
behind a single condition. If you declare either one in your own code, the whole
class is skipped and you silently lose the other.

Symptom: `@KafkaListener` methods are never invoked  -  no consumer group appears
in the broker at all, no error, nothing. Or: `NoSuchBeanDefinitionException: No
bean named 'kafkaListenerContainerFactory' available`.

Fix: provide both explicitly. Add `@EnableKafka` to the application class and
keep a manual `KafkaConsumerConfig` that declares `kafkaListenerContainerFactory`.

---

## @SpringBootTest(properties=...) does not reliably override test application.yml in SB4

Tried to override `spring.flyway.enabled=true` via `@SpringBootTest(properties=...)`.
Test `application.yml` had `flyway.enabled: false`. In SB4 the yml won  -  Flyway
never ran, tables didn't exist, first DB call failed.

Fix: run Flyway programmatically inside `@DynamicPropertySource`, before the
Spring context starts. `@DynamicPropertySource` runs after containers are up
but before any bean is created, so the schema exists by the time JPA touches it.

---

## KafkaTopicConfig beans create topics too late for Kafka Streams

`KafkaTopicConfig` declares topics as `NewTopic` beans. Spring Kafka's `KafkaAdmin`
picks these up and creates them  -  but only after the context is fully refreshed.
Kafka Streams validates that source topics exist during context refresh, before
`KafkaAdmin` has run. Result: `MissingSourceTopicException` at startup.

Fix: create the required topics via `AdminClient` inside `@DynamicPropertySource`,
before the context starts.

---

## Kafka Streams locks its state directory between test contexts

Kafka Streams writes to `/tmp/kafka-streams` and holds a file lock while running.
If two test contexts start in the same JVM (which Maven does), the second one
cannot acquire the lock and crashes.

Fix: `@DirtiesContext` on the Testcontainers test class. This forces Spring to
fully close that context (releasing the lock) before the next test class starts.
