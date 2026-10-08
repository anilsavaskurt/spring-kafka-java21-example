# spring-kafka-java21-example

[English](README.md) | **Türkçe**

**Spring Boot 3.5**, **Apache Kafka** ve **Java 21** ile olay güdümlü (event-driven) sipariş işleme.

Bir REST API siparişleri PostgreSQL'e kaydeder ve Kafka'ya olay (event) gönderir. Bir consumer bu olayları
işler. Geçici hataları ayrı retry topic'lerinde tekrar dener, sürekli hata veren mesajları da bir
dead-letter topic'e (DLT) yollar.

## Özellikler

| Konu | Nerede |
|---|---|
| Olay modeli olarak sealed interface + record'lar | `domain/OrderEvent.java` |
| Olaylar üzerinde eksiksiz (exhaustive) pattern-matching `switch` | `messaging/OrderEventListener.java` |
| Record pattern'leri (`instanceof OrderItem(var sku, ...)`) | `messaging/OrderEventListener.java` |
| Virtual thread'ler (`spring.threads.virtual.enabled`) | `application.yml` |
| Type header'ı olmadan, `type` alanıyla polimorfik JSON | `domain/OrderEvent.java`, `application.yml` |
| `@RetryableTopic` + `@DltHandler` ile bloklamayan yeniden denemeler | `messaging/OrderEventListener.java` |
| Bozuk (poison) mesajlar için `ErrorHandlingDeserializer` | `application.yml` |
| Idempotent producer, record key olarak sipariş id'si | `messaging/OrderEventPublisher.java` |
| Siparişlerin Spring Data JPA + PostgreSQL ile saklanması | `order/OrderEntity.java`, `order/OrderStore.java` |
| Testcontainers + `@ServiceConnection` ile entegrasyon testleri | `OrderFlowIntegrationTest.java` |

## Akış

```
POST /api/orders ──► OrderService ──► [orders] ──► OrderEventListener ──► CONFIRMED
                     (PENDING)           │ hata
                                         ▼
                     [orders-retry-0] ► [orders-retry-1] ► [orders-retry-2] ► [orders-dlt] ──► FAILED
```

Sipariş durumları: `PENDING` → `CONFIRMED` / `CANCELLED` / `FAILED`.

## Gereksinimler

- JDK 21
- Docker (Kafka, PostgreSQL ve entegrasyon testleri için)

Maven kurmanız gerekmez, proje Maven wrapper ile birlikte gelir.

## Çalıştırma

```bash
docker compose up -d          # Kafka, Kafka UI, PostgreSQL ve pgAdmin
./mvnw spring-boot:run        # Windows: mvnw.cmd spring-boot:run
```

`orders` tablosu uygulama açılırken Hibernate tarafından oluşturulur (`ddl-auto: update`).

| Servis | Adres | Giriş |
|---|---|---|
| Swagger UI | http://localhost:8080/swagger-ui.html | |
| OpenAPI tanımı | http://localhost:8080/v3/api-docs | |
| Kafka | `localhost:9092` | |
| Kafka UI | http://localhost:8081 | |
| PostgreSQL | `localhost:5432`, veritabanı `orderdb` | `appuser` / `secret` |
| pgAdmin | http://localhost:5050 | `admin@admin.com` / `admin` |

pgAdmin'de host `postgres`, port `5432` ve yukarıdaki PostgreSQL bilgileriyle bir sunucu ekleyin.

Bu bilgiler yerel geliştirme için varsayılan değerlerdir ve tüm portlar sadece `127.0.0.1`'e açıktır.
Değiştirmek için `docker-compose.yml`'in yanına bir `.env` dosyası oluşturun (git'e eklenmez):

```properties
POSTGRES_PASSWORD=...
PGADMIN_EMAIL=...
PGADMIN_PASSWORD=...
```

Ardından uygulamayı aynı şifreyi içeren `SPRING_DATASOURCE_PASSWORD` ortam değişkeniyle başlatın.

## Deneyin

http://localhost:8080/swagger-ui.html adresindeki Swagger UI'ı ya da curl'ü kullanın:

```bash
# Sipariş oluştur -> 202 Accepted, durum PENDING
curl -i -X POST localhost:8080/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "customer-1",
  "items": [{ "sku": "BOOK-1", "quantity": 2, "unitPrice": 12.50 }]
}'

# Kısa süre sonra durum CONFIRMED olur
curl localhost:8080/api/orders/{id}

# Siparişi iptal et
curl -X POST localhost:8080/api/orders/{id}/cancel -H "Content-Type: application/json" \
  -d '{ "reason": "changed my mind" }'

# sku'su OUT-OF-STOCK olan ürün her seferinde hata verir: 3 kez tekrar denenir, sonra DLT üzerinden FAILED olur
curl -X POST localhost:8080/api/orders -H "Content-Type: application/json" -d '{
  "customerId": "customer-2",
  "items": [{ "sku": "OUT-OF-STOCK", "quantity": 1, "unitPrice": 5 }]
}'
```

`orders`, `orders-retry-*` ve `orders-dlt` topic'lerini izlemek için http://localhost:8081 adresinden Kafka UI'ı açın.

## Testler

```bash
./mvnw test
```

`OrderEventJsonTest` Docker olmadan çalışır. `OrderFlowIntegrationTest` kendi Kafka ve PostgreSQL
container'larını başlatır, bu yüzden docker-compose ortamına ihtiyaç duymaz ve ona dokunmaz.

## Notlar

- Örneği küçük tutmak için producer ve consumer aynı uygulamada çalışır ve tek bir veritabanını paylaşır.
  Gerçek bir sistemde bunlar kendi veritabanları olan ayrı servisler olurdu.
- Siparişi kaydetmek ve olayı göndermek atomik değildir: uygulama ikisinin arasında çökerse sipariş
  `PENDING` olarak kalır. Bu boşluğu transactional outbox deseni kapatır.
- Retry topic'leri, partition'ı bloklamamak için kesin sıralamadan vazgeçer: tekrar denenen bir olay,
  aynı siparişin daha yeni bir olayından sonra işlenebilir.
