# Tài liệu nghiệp vụ và thiết kế kỹ thuật POS Cafe/Nhà hàng

## 1. Mục tiêu và phạm vi

Hệ thống POS phục vụ quán cafe/nhà hàng, hỗ trợ bán tại quầy, phục vụ tại bàn, mang đi, giao hàng, thanh toán, bếp, tồn kho và báo cáo. Kiến trúc mục tiêu dùng .NET 10, Clean Architecture, CQRS, Event-Driven Architecture với Kafka và DDD. `Order` là bounded context trọng tâm; các context khác giao tiếp qua API hoặc integration event, không truy cập database của nhau.

Phạm vi phiên bản đầu gồm: quản lý danh mục, cửa hàng/khu vực/bàn, ca làm việc, nhân viên và quyền, tạo và xử lý đơn, gửi món xuống bếp, thanh toán, hoàn/hủy, tồn kho tối thiểu, khuyến mãi cơ bản và báo cáo vận hành.

## 2. Bounded Context và cấu trúc repository

| Context | Trách nhiệm chính | Project dự kiến |
|---|---|---|
| Identity & Access | Người dùng, nhân viên, role, permission, token, refresh token | `src/Services/Identity/` |
| Catalog | Sản phẩm, category, modifier, giá, thuế, trạng thái bán | `src/Services/Catalog/` |
| Order | Vòng đời đơn hàng, dòng món, giá snapshot, trạng thái phục vụ | `src/Services/Order/` |
| Payment | Payment intent, giao dịch, refund, đối soát | `src/Services/Payment/` |
| Inventory | Nguyên liệu, recipe, tồn kho, xuất/nhập, cảnh báo | `src/Services/Inventory/` |
| Table/Store | Chi nhánh, khu vực, bàn, QR, trạng thái bàn | `src/Services/Table/` |
| Kitchen | Ticket bếp, station, hàng đợi chế biến, thời gian phục vụ | `src/Services/Kitchen/` |
| Reporting | Read model, doanh thu, món bán chạy, KPI | `src/Services/Reporting/` |

Các phần đã có trong repository gồm `src/Services/Catalog`, `src/Services/Order`, `src/Services/Payment`, `src/Gateway/PosCafe.ApiGateway`, `src/PosCafe.AppHost`, `src/PosCafe.ServiceDefaults` và `BuildingBlocks/BuildingBlocks`. Identity, Inventory, Table, Kitchen và Reporting là các context cần bổ sung. Mỗi service nên có `Api`, `Application`, `Domain`, `Infrastructure`.

## 3. Nghiệp vụ cốt lõi

### 3.1. Cấu hình cửa hàng và ca

Quản trị viên tạo chi nhánh, timezone, tiền tệ, thuế, phương thức thanh toán, khu vực và bàn. Nhân viên đăng nhập, mở ca bằng tiền đầu ca; trong ca có các giao dịch bán, hoàn, chi/thu khác. Cuối ca hệ thống khóa giao dịch mới, tính tiền mặt kỳ vọng, ghi nhận tiền thực tế và tạo chênh lệch cần duyệt.

### 3.2. Danh mục và giá

Một sản phẩm có SKU, tên, category, đơn vị bán, giá, thuế và trạng thái `Active/Inactive`. Modifier group có các lựa chọn như size, topping, mức đá/đường; mỗi lựa chọn có thể cộng giá và giới hạn số lượng. Giá và thuế phải được snapshot vào `OrderLine` khi đặt món để lịch sử đơn không thay đổi khi Catalog cập nhật.

### 3.3. Bàn, kênh bán và đơn hàng

Đơn có kênh `DineIn`, `TakeAway`, `Delivery`, `QrOrder` và có thể gắn bàn, khách hàng, nhân viên, ca và ghi chú. Một đơn gồm nhiều line; mỗi line giữ product ID, tên, SKU, giá gốc, giảm giá, thuế, modifier snapshot và số lượng.

Luồng chuẩn: tạo nháp -> thêm món -> xác nhận -> giữ/kiểm tra tồn kho -> gửi bếp -> chế biến -> sẵn sàng -> phục vụ/hoàn tất -> thanh toán -> đóng đơn. `Cancelled` và `Refunded` là các nhánh riêng có điều kiện quyền hạn và lý do bắt buộc. Không cho sửa trực tiếp đơn đã đóng; hoàn tiền tạo giao dịch bù trừ.

### 3.4. Bếp và phục vụ

Khi đơn được xác nhận, mỗi line cần chế biến phát sinh kitchen ticket theo station. Bếp nhận, bắt đầu, hoàn tất hoặc báo hết món. Nhân viên phục vụ có thể đánh dấu đã phục vụ; hệ thống lưu thời gian từng trạng thái để đo SLA. Lỗi hoặc hết món phải phát ra sự kiện để Order chuyển line sang `Unavailable` hoặc yêu cầu thay thế, không tự ý xóa lịch sử.

### 3.5. Thanh toán, hoàn và đối soát

Hỗ trợ tiền mặt, thẻ, QR/e-wallet và thanh toán kết hợp. Payment là context sở hữu trạng thái giao dịch; Order chỉ giữ trạng thái nghiệp vụ thanh toán và tổng tiền. Thanh toán online phải idempotent theo `OrderId + IdempotencyKey`; webhook phải xác thực chữ ký, chống replay và xử lý được đến trễ. Chỉ đóng đơn khi đủ số tiền đã thanh toán hoặc được quyền ghi công nợ.

### 3.6. Khuyến mãi và giá trị đơn

Tổng tiền được tính theo thứ tự: subtotal -> giảm theo line -> giảm theo đơn -> phí -> thuế -> rounding -> grand total. Promotion phải có thời gian hiệu lực, phạm vi áp dụng, giới hạn lượt dùng và ưu tiên rõ ràng. Tại thời điểm xác nhận, Order lưu promotion snapshot và không phụ thuộc việc tính lại từ Catalog.

## 4. DDD cho Order

### 4.1. Aggregate và invariant

`Order` là aggregate root. Các entity/value object chính: `OrderLine`, `ModifierSelection`, `Money`, `Address`, `OrderId`, `CustomerId`, `TableId`, `OrderStatus`, `PaymentStatus`. Repository chỉ có `IOrderRepository` cho aggregate; không expose query kiểu ORM từ Domain.

Invariant quan trọng:

- Mỗi đơn có một chi nhánh, kênh bán và currency hợp lệ.
- Số lượng line dương; không trùng line theo cùng product và modifier snapshot nếu nghiệp vụ yêu cầu gộp.
- Đơn đã `Confirmed` không được thay đổi giá, thuế hoặc line tùy tiện.
- Chỉ chuyển trạng thái theo state machine hợp lệ; `Completed`, `Cancelled`, `Refunded` là trạng thái kết thúc tương ứng.
- Grand total không âm và bằng tổng các thành phần sau rounding.
- Chỉ user có permission phù hợp mới được hủy, giảm giá vượt ngưỡng hoặc hoàn tiền.

Domain methods nên có dạng `order.AddLine(...)`, `order.Confirm(...)`, `order.Cancel(...)`, `order.MarkReady(...)`; tránh setter công khai. Mỗi thay đổi nghiệp vụ tạo domain event như `OrderCreated`, `OrderConfirmed`, `OrderCancelled`, `OrderCompleted`.

### 4.2. CQRS và use case

Command handler đặt tại `Application` và chỉ thay đổi aggregate qua domain method:

- `CreateOrderCommand`, `AddOrderLineCommand`, `RemoveOrderLineCommand`
- `ConfirmOrderCommand`, `SendOrderToKitchenCommand`, `MarkOrderServedCommand`
- `CancelOrderCommand`, `ApplyDiscountCommand`, `CompleteOrderCommand`
- `RecordPaymentCommand`, `RequestRefundCommand`

Query không đọc aggregate bằng cách hydrate đầy đủ. Dùng read model riêng cho `GetOrderById`, `SearchOrders`, `GetOpenOrders`, `GetKitchenQueue`, `GetDailySales`. Có thể dùng MediatR hoặc dispatcher nội bộ, FluentValidation cho command và pipeline behavior cho logging, authorization, validation, transaction và idempotency.

### 4.3. Transaction và Outbox

Trong một transaction database, handler cập nhật Order và ghi `OutboxMessage` chứa event chưa gửi. Background publisher đọc outbox, publish Kafka, rồi đánh dấu `PublishedAt`; không publish Kafka trực tiếp giữa transaction nghiệp vụ. Consumer lưu `InboxMessage` hoặc consumer offset theo event ID để xử lý ít nhất một lần nhưng hiệu ứng nghiệp vụ chỉ một lần.

## 5. Kiến trúc kỹ thuật

Gateway dùng để xác thực JWT, rate limit, correlation ID và route đến service; không chứa nghiệp vụ. Mỗi service áp dụng:

```text
Api -> Application -> Domain
                 -> Infrastructure
```

`Infrastructure` chứa EF Core DbContext, migrations, Kafka, external provider và repository. Mỗi bounded context sở hữu database/schema riêng. PostgreSQL là lựa chọn phù hợp cho transactional data; MongoDB dùng cho service đơn giản hoặc read model cần schema linh hoạt và query nhanh; Redis dùng cho cache ngắn hạn và distributed lock có kiểm soát. Reporting dùng read database hoặc projection riêng.

Target framework là `net10.0`, bật nullable và implicit usings. `ServiceDefaults` cung cấp OpenTelemetry, health checks, resilience, service discovery và HTTP defaults. `AppHost` khai báo các resource local như database, Kafka và service dependencies.

### 5.1. Polyglot Persistence: MongoDB

Không dùng MongoDB thay thế mặc định cho mọi service. PostgreSQL phù hợp với transaction nhiều bước, invariant chặt và payment ledger. MongoDB phù hợp với document có schema linh hoạt, read model denormalized, catalog projection, kitchen ticket view, audit/search document và reporting. Redis dành cho cache, TTL và rate limit.

`Order` write model và `Payment` ledger nên tiếp tục dùng PostgreSQL. Nếu cần đọc Order nhanh, xây `OrderSummaryProjection` trong MongoDB từ Kafka thay vì cho service khác truy cập database Order.

Cấu trúc đề xuất:

```text
src/Services/Reporting/
  PosCafe.Reporting.Api/
  PosCafe.Reporting.Application/
  PosCafe.Reporting.Domain/
  PosCafe.Reporting.Infrastructure/
    Mongo/
      ReportingMongoContext.cs
      Configurations/
      Repositories/
```

`Infrastructure` tham chiếu `MongoDB.Driver`, đăng ký `IMongoClient` singleton và database theo cấu hình. Domain không tham chiếu MongoDB; document mapper và repository nằm ở Infrastructure. Document phải tối ưu cho query, giới hạn kích thước và không chứa unbounded array.

Thiết kế index theo query chính, ví dụ `{ storeId: 1, status: 1, createdAt: -1 }` và `{ orderId: 1 }` unique. Dùng projection chỉ lấy trường cần thiết và cursor pagination thay vì `Skip` lớn. Mỗi bounded context sở hữu database/collection riêng, không truy cập collection của context khác.

MongoDB read model là eventually consistent. Consumer phải idempotent theo `eventId`, xử lý event đến trễ, có thể rebuild bằng cách replay Kafka và theo dõi projection lag. Không dùng read model MongoDB cho quyết định cần transaction tức thời.
Reporting ghi event ledger và daily sales trong cùng Mongo transaction; môi trường production bắt buộc dùng MongoDB replica set hoặc sharded cluster. Không deploy projector này trên MongoDB standalone vì transaction đa document sẽ không được hỗ trợ.
AppHost local khởi động Mongo với replica set `rs0` và mount `src/PosCafe.AppHost/mongo-init.js` để gọi `rs.initiate()`. Production phải dùng chart/operator quản lý replica set tương đương và bảo đảm hostname trong replica config có thể resolve từ tất cả application pods.

Trong `AppHost`, khai báo MongoDB resource với volume bền vững và connection reference cho service sử dụng. Production cần secret manager, authentication, TLS, replica set, backup và monitoring. Thêm readiness health check cùng metric query latency, connection pool, replication lag và projection lag.

### 5.2. Identity Service

Identity quản lý `User`, `EmployeeProfile`, `Role`, `Permission`, `RefreshToken`, `AuditLog`. Authentication dùng ASP.NET Core Identity hoặc giải pháp OIDC tương thích; access token ngắn hạn, refresh token rotation, revoke khi logout/đổi mật khẩu. JWT claims tối thiểu gồm `sub`, `tenant/store scopes`, `roles`, `permissions`, `jti`.

API chính: `POST /identity/login`, `POST /identity/refresh`, `POST /identity/logout`, `GET /identity/me`, quản trị user/role/permission và reset password. Password không tự xây dựng thuật toán; dùng password hasher chuẩn, secret lưu secret manager. Gateway xác thực token, còn service vẫn phải kiểm tra authorization ở application boundary.

### 5.3. Building Blocks

`BuildingBlocks/BuildingBlocks` nên cung cấp các abstraction dùng chung nhưng không chứa logic riêng của Order:

- `Domain`: `Entity`, `AggregateRoot`, `ValueObject`, `IDomainEvent`, `Result`, error codes.
- `Application`: `ICommand`, `IQuery`, handler, validation, authorization, transaction behavior.
- `Infrastructure`: `BaseDbContext`, outbox/inbox, audit, clock, current user, idempotency.
- `Messaging`: event envelope, Kafka producer/consumer, serializer, retry, dead-letter.
- `Observability`: correlation/causation ID, logging scope, metrics và tracing.

Building Block phải ổn định, ít phụ thuộc; không tạo một shared domain model khiến các context bị ghép chặt.

## 6. Kafka và hợp đồng sự kiện

Event dùng integration contract bất biến, version hóa (`v1`), có envelope:

```json
{
  "eventId": "uuid",
  "eventType": "OrderConfirmed.v1",
  "occurredAt": "2026-08-30T12:00:00Z",
  "correlationId": "uuid",
  "causationId": "uuid",
  "tenantId": "uuid",
  "aggregateId": "order-id",
  "schemaVersion": 1,
  "payload": {}
}
```

Topic nên theo bounded context, ví dụ `pos.order.events`, `pos.payment.events`, `pos.catalog.events`, `pos.kitchen.events`; key là `aggregateId` để giữ thứ tự trong một Order. Consumer phải idempotent, retry có backoff, đưa message lỗi vào DLQ và có cơ chế replay sau khi sửa lỗi. Không gửi dữ liệu thẻ, password hoặc secret vào event.

Một số event chính: `CatalogProductChanged`, `OrderConfirmed`, `OrderCancelled`, `OrderSubmittedToKitchen`, `KitchenTicketReady`, `PaymentAuthorized`, `PaymentFailed`, `PaymentRefunded`, `InventoryReserved`, `InventoryReservationFailed`, `ShiftClosed`. Event chỉ thông báo sự kiện đã xảy ra; command giữa service dùng API hoặc message command riêng khi cần.

## 7. API và dữ liệu

API dùng REST/JSON, version theo `/api/v1`, ProblemDetails, pagination bằng cursor hoặc page/size, optimistic concurrency với `rowVersion`/ETag. Các command tạo hoặc thanh toán phải nhận `Idempotency-Key`. Không trả entity EF trực tiếp; dùng DTO.

Order tables tối thiểu: `orders`, `order_lines`, `order_line_modifiers`, `order_status_history`, `outbox_messages`, `inbox_messages`, `idempotency_records`. Index theo `(store_id, status, created_at)`, `order_number`, `customer_id` và `external_reference`. Tiền dùng decimal với currency; thời gian lưu UTC và hiển thị theo timezone cửa hàng.

## 7.1. Outbox và Inbox production

`Order` và `Payment` có database PostgreSQL riêng. Command handler mở transaction, lưu aggregate và `outbox_messages` cùng lúc; publisher không tham gia transaction nghiệp vụ. Worker claim message trong transaction bằng `FOR UPDATE SKIP LOCKED`, cập nhật lease/attempt rồi commit trước khi publish Kafka; nhờ đó nhiều replica không cùng xử lý một row. Publisher dùng `Acks.All`, idempotent producer và key là `aggregateId`. Thành công ghi `ProcessedOnUtc`; lỗi ghi `Attempts`, `Error`, gia hạn lease theo exponential backoff. Sau `MaxAttempts`, ghi `DeadLetteredOnUtc` để vận hành xử lý và replay có kiểm soát.

Topic chuẩn là `pos.order.events` và `pos.payment.events`. Envelope phải có `eventId`, `eventType`, `aggregateId`, `occurredAt`, `correlationId`, `causationId`, `schemaVersion` và `payload`; Kafka header hiện thực các trường định danh bằng `event-id`, `event-type`, `correlation-id` và `schema-version=1`. Không gửi secret hoặc dữ liệu thẻ. Consumer tắt auto-commit, kiểm tra ID/type/schema version trước Inbox, bắt đầu Inbox bằng unique key `(eventId, consumer)`, thực hiện side effect trong cùng transaction, rồi mới commit offset Kafka. Event lỗi không commit offset để retry; delay retry phải lấy từ cấu hình môi trường, poison message phải được quan sát và đưa vào DLQ. Consumer chỉ chấp nhận version được hỗ trợ; version lạ hoặc thiếu header phải vào DLQ, không được âm thầm bỏ qua.
Schema canonical và compatibility declaration được lưu tại `schemas/`; `schemas/order-confirmed.v1.schema.json` là contract của `OrderConfirmed.v1`, còn `schemas/order-confirmed.v1.compatibility.json` khai báo backward compatibility và required headers. Pipeline triển khai phải đăng ký/verify schema trước rollout producer.

Consumer Payment retry tối đa `ConsumerMaxAttempts`; backoff exponential có trần `RetryMaxSeconds`; sau ngưỡng này publish nguyên message sang `DeadLetterTopic` kèm event ID, attempt cuối, topic/partition/offset gốc và error header, rồi commit offset nguồn để không chặn partition. Nếu publish DLQ thất bại, không commit offset và tiếp tục retry. Consumer đặt `MaxPollIntervalMs` theo trần backoff để tránh rebalance trong lúc retry. DLQ phải có dashboard, alert và runbook replay sau khi sửa lỗi.
Producer Kafka dùng cấu hình chung trong `BuildingBlocks`: idempotence, `acks=all`, tối đa 10 lần retry, retry backoff 500ms, request timeout 30 giây, message timeout 120 giây, Snappy compression và tắt auto-create topic. Hạ tầng phải provision topic/partition/replication/ACL trước startup; ứng dụng không tự tạo topic.
Readiness của Order, Payment, Inventory và Reporting gọi Kafka AdminClient để verify `Messaging:RequiredTopics` và `Messaging:MinimumTopicPartitions`; topic thiếu, partition không đủ hoặc ACL không cho metadata sẽ chặn traffic. Đây là kiểm tra metadata, không phải quyền tạo topic, nên production vẫn phải provision topic bằng IaC/migration hạ tầng.
Kafka security được cấu hình tập trung dưới `Kafka:Security` và áp dụng đồng nhất cho producer, consumer và AdminClient. Production ưu tiên `SaslSsl`/`Ssl`, CA verification luôn bật, credential/certificate lấy từ secret manager, không lưu trong `appsettings*.json`; ACL phải cấp riêng quyền read cho consumer group, write cho producer topic và describe cho readiness check.
Database security follows the same rule: PostgreSQL and MongoDB connection strings are injected from a secret manager and enable certificate verification in production. PostgreSQL uses `SSL Mode=VerifyFull`; MongoDB uses `tls=true` with a trusted CA and replica-set settings. Development-only local credentials must never be valid in a production environment.

Inventory consumer áp dụng cùng chính sách: attempt được lưu trong `inbox_messages`, side effect reserve tồn kho nằm cùng transaction với Inbox, retry exponential backoff có giới hạn `RetryMaxSeconds`, và sau `ConsumerMaxAttempts` publish nguyên message sang `pos.inventory.order-events.dlq` cùng metadata nguồn. Chỉ khi publish DLQ và ghi trạng thái terminal thành công mới commit offset; lỗi publish DLQ tiếp tục giữ offset để retry.

Reporting consumer lưu attempt và trạng thái xử lý trong Mongo collection `processed_reporting_events`; transaction Mongo bảo đảm event ledger và phép cộng doanh thu được commit cùng nhau. Poison event sau `ConsumerMaxAttempts` được chuyển sang `pos.reporting.order-events.dlq`, giữ nguyên payload/headers và chỉ commit Kafka offset sau khi ghi terminal state thành công.

Replay phải là thao tác có chủ đích theo từng `event-id`, không tự động replay toàn bộ DLQ. Dùng CLI `dotnet run --project tools/PosCafe.DlqReplay -- --bootstrap-servers localhost:9092 --source-topic pos.payment.order-events.dlq --target-topic pos.order.events --event-id <event-id>`; tool giữ nguyên key, payload và headers, chỉ commit offset sau khi publish thành công.

Migration là bắt buộc cho production: chạy `dotnet ef database update` hoặc pipeline migration trước khi start service. `EnsureCreated` chỉ dùng cho test database. Theo dõi outbox backlog, dead-letter count, consumer lag, retry count, publish latency và projection lag; cung cấp runbook replay sau khi sửa consumer.

CI phải chạy `dotnet restore`, build Release và test với PostgreSQL/Kafka services. Integration suite dùng database PostgreSQL riêng, chạy migrations thật và kiểm tra flow Order event qua Kafka vào Payment projection cùng Inbox idempotency. Production deployment không chạy `EnsureCreated`; chạy migration có kiểm soát, kiểm tra readiness rồi mới bật traffic. Khi Kafka unavailable, giữ message ở Outbox; khi consumer lỗi, không commit offset, sửa lỗi rồi replay từ offset/DLQ. Secret, JWT key và Kafka credential chỉ đi qua secret manager hoặc environment variables.

## 8. Bảo mật, vận hành và tiêu chí chấp nhận

Áp dụng least privilege, HTTPS, secret manager, audit cho thao tác nhạy cảm, masking PII, phân quyền theo store và chống truy cập chéo tenant. Health check phải phân biệt liveness/readiness. Dashboard cần latency, error rate, Kafka lag, outbox backlog, payment failure và order completion time.

Mọi API dùng `ServiceDefaults` đều phát hành `X-Correlation-Id`, security headers (`nosniff`, `DENY`, CSP tối thiểu) và global fixed-window rate limit theo IP. Có thể cấu hình bằng `Security__RateLimit__PermitLimit` và `Security__RateLimit__WindowSeconds`; production phải đặt giá trị qua environment hoặc secret manager, không commit secret. Gateway dùng YARP với route cấu hình tới Identity, Catalog, Order, Payment, Inventory, Store và Reporting; JWT được validate tại Gateway bằng `Jwt__Key` (tối thiểu 32 bytes), forward nguyên vẹn để downstream service xác thực lại. Chỉ register/login/refresh/logout là public; route còn lại yêu cầu policy `authenticated`. Mỗi cluster có timeout upstream 30 giây và giới hạn 100 connections/server. Không retry tự động các `POST` nghiệp vụ nếu chưa có idempotency policy riêng; chỉ retry `GET/HEAD/OPTIONS` sau khi bổ sung policy retry theo route.

Các service nghiệp vụ cũng bật JWT validation và fallback authorization policy, vì vậy truy cập trực tiếp không qua Gateway vẫn yêu cầu access token; chỉ health/liveness endpoints là anonymous. Identity lưu assignment trong `UserStoreAssignments`, phát hành nhiều claim `store_id` trong access token và cung cấp endpoint admin `POST /identity/admin/users/{userId}/stores` để cấp/thu hồi quyền cửa hàng. `admin` được phép vượt scope; user thường chỉ được đọc/thao tác dữ liệu của store có trong token. Khi mở rộng command mới, bắt buộc truyền `ClaimsPrincipal` vào boundary để kiểm tra scope trước khi gọi application service.

Role matrix hiện tại: Catalog mutation dùng `catalog-manager` hoặc `admin`; Store mutation dùng `store-manager` hoặc `admin`; Order command dùng `cashier`, `manager` hoặc `admin`; Payment command dùng `cashier`, `manager` hoặc `admin`; Inventory mutation dùng `inventory-manager`, `store-manager` hoặc `admin`. Order create/confirm/cancel đã kiểm tra `store_id` trước application service. Payment store-scope phải resolve từ payment/order projection trước khi bật enforcement, không tin `store_id` do client gửi.
Identity bootstrap tạo các role chuẩn (`admin`, `manager`, `cashier`, `catalog-manager`, `store-manager`, `inventory-manager`) khi service khởi động. Tài khoản admin chỉ được tạo nếu đồng thời có `Identity__Bootstrap__AdminEmail` và `Identity__Bootstrap__AdminPassword`; password chỉ truyền qua secret manager/environment và phải rotate sau bootstrap.

Order, Payment, Inventory, Catalog và Store ghi `audit_entries` trong cùng transaction với aggregate/state change và outbox nếu có. Audit index theo store/thời gian và entity/thời gian; production cần retention policy, quyền chỉ-ghi cho ứng dụng và quyền đọc giới hạn cho vận hành/compliance. Cả năm service có retention worker chạy cleanup ngay sau startup rồi mỗi 24 giờ, mặc định giữ 365 ngày; lỗi database được log và retry ở chu kỳ sau. Khi `Audit:Archive:Enabled=true`, worker upload batch JSONL lên Azure Blob trước và chỉ purge đúng các record đã upload thành công; nếu archive lỗi, dữ liệu ở PostgreSQL được giữ lại. Cấu hình bằng `Audit__RetentionDays`, `Audit__IntervalHours`, `Audit__Archive__ConnectionString`, `Audit__Archive__ContainerName`; bật immutable retention/WORM và lifecycle policy ở storage account. Identity cleanup refresh token chạy mặc định mỗi 6 giờ và giữ token revoked 7 ngày; cấu hình bằng `Security__RefreshTokenCleanup__IntervalHours` và `Security__RefreshTokenCleanup__RevokedRetentionDays`. Không ghi PAN, CVV, refresh token hoặc secret vào metadata audit.

Metric audit dùng meter `PosCafe.Messaging`: `poscafe.audit.archive.succeeded`, `poscafe.audit.archive.failures`, `poscafe.audit.archive.records` và `poscafe.audit.retention.failures`, đều có tag `service`. Nên alert khi archive hoặc retention failure tăng, đồng thời đối chiếu số record archived với số record purged. Metric Kafka hiện có published, publish failures, dead-lettered, duplicate, consumed và `poscafe.kafka.consumer.lag` (tag `service`, `topic`, `partition`) để theo dõi outbox/inbox, DLQ và độ trễ consumer thực tế so với high watermark.

Để scrape bằng Prometheus, đặt `Observability__Prometheus__Enabled=true` cho service và gọi `GET /metrics` bằng JWT có role `admin`; mặc định endpoint bị tắt. Aspire Dashboard vẫn nhận telemetry qua OTLP khi `OTEL_EXPORTER_OTLP_ENDPOINT` được cấu hình. Không expose `/metrics` trực tiếp ra Internet; dùng network policy hoặc mTLS ở ingress.

DLQ management nằm tại Gateway và dùng scope theo service/topic: Order cho `admin`, `manager`, `order-operator`; Payment cho `admin`, `manager`, `payment-operator`; Inventory cho `admin`, `store-manager`, `inventory-manager`; Reporting cho `admin`, `manager`. `GET /ops/dlq/routes` liệt kê route theo scope; `GET /ops/dlq/history` phân trang; `GET /ops/dlq/summary` aggregate theo topic/status trong tối đa 31 ngày; `POST /ops/dlq/replay` nhận `sourceTopic`, `targetTopic`, `eventId`. Service chỉ replay các cặp topic được allowlist, dùng Kafka producer `acks=all` và idempotence, giữ nguyên `event-id` để Inbox downstream loại bỏ xử lý trùng. Mọi replay được log cùng actor, correlation ID và offset; trước khi replay cần sửa nguyên nhân lỗi và theo dõi DLQ/consumer lag.
Replay history lưu trong `opsdb`, schema dùng EF migrations versioned `InitialOps`, `AddReplayLease` và `AddReplayLeaseFencing`, bắt buộc `Idempotency-Key`, có unique index để chống request trùng, claim `Pending` có lease 5 phút và fencing token; renew mỗi phút và conditional check ngay trước publish trong lúc scan, lưu trạng thái `Completed`, `Failed` hoặc `NotFound`, và retention worker mặc định 90 ngày. Lease hết hạn được chuyển thành `Failed` để operator điều tra, không tự động replay mù. Metric `poscafe.dlq.replays` phân loại outcome; `poscafe.dlq.retention.failures` cảnh báo lỗi cleanup.
Audit của Gateway trong `opsdb.audit_entries` có retention worker riêng, archive-before-purge qua `AuditArchiveClient`, mặc định giữ 365 ngày; lỗi archive/retention giữ nguyên dữ liệu PostgreSQL và phát metric cảnh báo. Metric `poscafe.audit.retention.purged` chỉ tăng sau khi purge thành công để đối chiếu volume archive/purge.
DLQ replay xuất gauge hiện trạng `poscafe.dlq.pending`, `poscafe.dlq.failed`, `poscafe.dlq.not_found` và `poscafe.dlq.completed`, được refresh từ `opsdb` sau mỗi retention cycle để dashboard phản ánh snapshot thực tế. Payment, Inventory và Reporting có readiness check consumer lag từ snapshot topic/partition; cấu hình ngưỡng bằng `Messaging__ConsumerLag__MaxLag` và `Messaging__ConsumerLag__MaxSnapshotAgeSeconds`.
Retry dùng `POST /ops/dlq/replay/{historyId}/retry`, chỉ nhận record `Failed`, bắt buộc idempotency key mới và tạo record lịch sử mới, không sửa record gốc. Audit DLQ lưu riêng trong `opsdb.audit_entries`, liên kết trực tiếp bằng `EntityId = DlqReplayRecord.Id`, có migration `AddDlqAuditEntries` và metadata tối thiểu cho actor, topic, event, offset, outcome, denied reason và correlation. Rate limit riêng cho DLQ dùng `Security__RateLimit__DlqPermitLimit` và `Security__RateLimit__DlqWindowSeconds`.

Definition of Done cho nghiệp vụ mới: có invariant/domain event, command/query và authorization, migration, outbox/inbox, unit test domain, integration test API/message, telemetry, tài liệu thay đổi và rollback migration. Chạy từ root:

```powershell
dotnet restore
dotnet build PosCafe.slnx
dotnet test PosCafe.slnx
dotnet run --project src/PosCafe.AppHost/PosCafe.AppHost.csproj
```

## 9. Lộ trình triển khai

1. Hoàn thiện BuildingBlocks, Identity, Store/Table và Catalog; thêm MongoDB resource trong AppHost.
2. Xây Order aggregate, command/query, database, outbox và API bán hàng.
3. Tích hợp Kafka, Kitchen và Payment với idempotency, retry, DLQ.
4. Bổ sung Inventory reservation, promotion, shift closing và refund.
5. Xây Reporting projections trên MongoDB, observability, load test, security test và disaster recovery.
