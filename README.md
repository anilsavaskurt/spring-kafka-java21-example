# spring-kafka-java21-example

**English** | [Türkçe](README.tr.md)

Event-driven order processing with **Spring Boot 3.5**, **Apache Kafka** and **Java 21**.

A REST API stores orders in PostgreSQL and publishes events to Kafka. A consumer processes them, retries
temporary failures on separate retry topics, and sends messages that keep failing to a dead-letter topic.

## Features

| Topic | Where |
|---|---|
| Sealed interface + records as the event model | `domain/OrderEvent.java` |
| Exhaustive pattern-matching `switch` over events | `messaging/OrderEventListener.java` |
| Record patterns (`instanceof OrderItem(var sku, ...)`) | `messaging/OrderEventListener.java` |
| Virtual threads (`spring.threads.virtual.enabled`) | `application.yml` |
| Polymorphic JSON with a `type` property, no type headers | `domain/OrderEvent.java`, `application.yml` |
| Non-blocking retries with `@RetryableTopic` + `@DltHandler` | `messaging/OrderEventListener.java` |
| `ErrorHandlingDeserializer` for poison messages | `application.yml` |
| Idempotent producer, order id as record key | `messaging/OrderEventPublisher.java` |
| Orders persisted with Spring Data JPA + PostgreSQL | `order/OrderEntity.java`, `order/OrderStore.java` |
| Integration tests with Testcontainers + `@ServiceConnection` | `OrderFlowIntegrationTest.java` |

## Flow

```
POST /api/orders ──► OrderService ──► [orders] ──► OrderEventListener ──► CONFIRMED
                     (PENDING)           │ failure
                                         ▼
                     [orders-retry-0] ► [orders-retry-1] ► [orders-retry-2] ► [orders-dlt] ──► FAILED
```

Order statuses: `PENDING` → `CONFIRMED` / `CANCELLED` / `FAILED`.

## Requirements

- JDK 21
- Docker (for Kafka, PostgreSQL and the integration tests)

Maven is not needed; the project ships with the Maven wrapper.

## Run

```bash
docker compose up -d          # Kafka, Kafka UI, PostgreSQL and pgAdmin
./mvnw spring-boot:run        # Windows: mvnw.cmd spring-boot:run
```

The `orders` table is created by Hibernate on startup (`ddl-auto: update`).

| Service | Address | Login |
|---|---|---|
| Swagger UI | http://localhost:8080/swagger-ui.html | |
| OpenAPI spec | http://localhost:8080/v3/api-docs | |
| Kafka | `localhost:9092` | |
| Kafka UI | http://localhost:8081 | |
| PostgreSQL | `localhost:5432`, database `orderdb` | `appuser` / `secret` |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `admin` |

In pgAdmin, register a server with host `postgres`, port `5432` and the PostgreSQL login above.

These credentials are local development defaults, and all ports are bound to `127.0.0.1`.
To change them, create a `.env` file next to `docker-compose.yml` (it is git-ignored):

```properties
POSTGRES_PASSWORD=...
PGADMIN_EMAIL=...
PGADMIN_PASSWORD=...
```

and start the app with the matching `SPRING_DATASOURCE_PASSWORD` environment variable.

## Try it

Use Swagger UI at http://localhost:8080/swagger-ui.html, or curl:

```bash
# Create an order -> 202 Accepted, status PENDING
curl -i -X POST localhost:8080/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "customer-1",
  "items": [{ "sku": "BOOK-1", "quantity": 2, "unitPrice": 12.50 }]
}'

# A moment later the status is CONFIRMED
curl localhost:8080/api/orders/{id}

# Cancel it
curl -X POST localhost:8080/api/orders/{id}/cancel -H "Content-Type: application/json" \
  -d '{ "reason": "changed my mind" }'

# An item with sku OUT-OF-STOCK fails every time: retried 3 times, then FAILED via the DLT
curl -X POST localhost:8080/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "customer-2",
  "items": [{ "sku": "OUT-OF-STOCK", "quantity": 1, "unitPrice": 5 }]
}'
```

Open Kafka UI at http://localhost:8081 to watch the `orders`, `orders-retry-*` and `orders-dlt` topics.

## Tests

```bash
./mvnw test
```

`OrderEventJsonTest` runs without Docker. `OrderFlowIntegrationTest` starts its own Kafka and
PostgreSQL containers, so it does not need (or touch) the docker-compose environment.

## Notes

- The producer and consumer live in the same app and share one database to keep the example
  small. In a real system they would be separate services with their own databases.
- Saving the order and publishing the event are not atomic: if the app crashes in between, the
  order stays `PENDING`. A transactional outbox would close that gap.
- Retry topics trade strict ordering for not blocking the partition: a retried event can be
  processed after a newer event of the same order.
