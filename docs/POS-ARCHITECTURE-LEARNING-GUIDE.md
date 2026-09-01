# Hướng Dẫn Kiến Trúc, Luồng Vận Hành Và Tư Duy Deploy PosCafe

## Mục tiêu

Đây là tài liệu học thực chiến dành cho người mới tham gia PosCafe. Mục tiêu không phải ghi lại mọi dòng code, mà là giúp bạn:

- Đọc được một request từ Gateway đến database và Kafka.
- Hiểu vì sao code được chia thành Domain, Application và Infrastructure.
- Biết Outbox, Inbox, retry, DLQ, Saga và eventual consistency hoạt động như thế nào.
- Biết lấy correlation ID ở đâu và debug một flow phân tán.
- Hiểu cách một thay đổi đi từ code đến production an toàn.
- Hình thành cách đặt câu hỏi của một senior khi thiết kế hoặc review feature.

Tài liệu bám theo code hiện tại. Chỗ nào repository chưa có Orchestrator, payment gateway thật, backup platform hoặc managed Kafka thì tài liệu sẽ nói rõ, không giả định rằng nó đã được triển khai.

## Mục lục học nhanh

1. Phase 0: Bản đồ repository và cách đọc một feature.
2. Phase 1: Nghiệp vụ POS và DDD.
3. Phase 2: CQRS, API, exception và idempotency.
4. Phase 3: Kafka, Outbox, Inbox, retry, DLQ và Saga.
5. Phase 4: PostgreSQL, MongoDB và consistency.
6. Phase 5: Observability, security và audit.
7. Phase 6: Tư duy deploy production.
8. Phase 7: Debug một flow hoàn chỉnh.
9. Phase 8: Tư duy Middle lên Senior.
10. Glossary và bản đồ file nguồn.

---

# Phase 0: Bản Đồ Repository Và Cách Đọc Code

## 0.1. Cấu trúc solution

Repository là một .NET 10 solution tổ chức theo service boundary:

| Khu vực | Vai trò |
|---|---|
| src/PosCafe.AppHost/ | Aspire AppHost, điều phối môi trường local |
| src/PosCafe.ServiceDefaults/ | Health, OpenTelemetry, rate limit, service discovery và middleware dùng chung |
| src/Gateway/PosCafe.ApiGateway/ | YARP Gateway, JWT validation, operations và DLQ replay |
| src/Services/Order/ | Order bounded context |
| src/Services/Payment/ | Payment bounded context và consumer Order event |
| src/Services/Inventory/ | Inventory bounded context và consumer Order event |
| src/Services/Reporting/ | MongoDB read model và consumer reporting |
| src/Services/Catalog/ | Category/Product |
| src/Services/Store/ | Store và store scope |
| src/Services/Identity/ | User, role, JWT và refresh token |
| src/Services/Kitchen/ | Kitchen API baseline |
| BuildingBlocks/BuildingBlocks/ | Domain, Exception, Messaging, Observability dùng chung |
| deploy/ | Docker, Compose, Helm, Kustomize và migration job |
| ops/ | Prometheus, Grafana, alert và runbook |
| schemas/ | JSON schema cho integration event |

Một service không nhất thiết phải có đủ mọi layer nếu nghiệp vụ đơn giản. Tuy nhiên nguyên tắc quan trọng là database và invariant thuộc về bounded context, không bị service khác sửa trực tiếp.

## 0.2. Aspire làm gì?

Aspire là công cụ orchestration cho local development. Nó:

- Khởi tạo resource PostgreSQL, MongoDB và Kafka.
- Tạo logical database reference.
- Cấp connection/resource reference cho API.
- Mô hình hóa thứ tự khởi động bằng WaitFor.
- Hiển thị resource, logs, health và dependency trong Aspire dashboard.

Aspire không phải business orchestrator và không phải production Kubernetes. Production dùng artifact/manifests trong deploy/.

Code thực tế trong src/PosCafe.AppHost/AppHost.cs:

~~~csharp
var postgres = builder.AddPostgres("postgres");
var mongo = builder.AddMongoDB("mongo")
    .WithArgs("--replSet", "rs0", "--bind_ip_all")
    .WithInitFiles("mongo-init.js");
var kafka = builder.AddKafka("kafka");

var orderDb = postgres.AddDatabase("orderdb");

var order = builder.AddProject<Projects.PosCafe_Order_Api>("order")
    .WithReference(orderDb)
    .WithReference(kafka)
    .WaitFor(postgres)
    .WaitFor(kafka);
~~~

Cách đọc đoạn trên:

1. Tạo resource PostgreSQL và Kafka.
2. Tạo database logic orderdb trong PostgreSQL resource.
3. Đăng ký Order API.
4. Aspire inject reference cho orderdb và kafka.
5. Order chỉ được khởi động sau dependency local đã sẵn sàng.

Chạy local:

~~~text
dotnet run --project src/PosCafe.AppHost/PosCafe.AppHost.csproj
~~~

## 0.3. Cách lần theo một feature

Khi học một feature, không mở tất cả file cùng lúc. Dùng quy trình:

1. Tìm endpoint trong Program.cs.
2. Xác định request DTO hoặc command.
3. Tìm application interface/service được gọi.
4. Đọc aggregate/domain invariant.
5. Đọc DbContext và transaction.
6. Đọc audit và outbox record.
7. Tìm publisher/consumer bằng tên topic.
8. Đọc health check và metric liên quan.
9. Đọc migration và deployment config.

Ví dụ bắt đầu từ endpoint tạo Order:

~~~csharp
app.MapPost("/api/v1/orders",
    async (CreateOrderCommand command,
           IOrderCommandService service,
           ClaimsPrincipal principal,
           HttpRequest request,
           CancellationToken ct) =>
{
    if (!principal.CanAccessStore(command.StoreId))
        return Results.Forbid();

    var idempotencyKey = request.Headers["Idempotency-Key"].ToString();
    var result = await service.CreateAsync(command, idempotencyKey, requestHash, ct);
    return Results.Created($"/api/v1/orders/{result.OrderId}", result);
});
~~~

Từ đây lần lượt mở:

- src/Services/Order/PosCafe.Order.Api/Program.cs
- src/Services/Order/PosCafe.Order.Application/OrderCommands.cs
- src/Services/Order/PosCafe.Order.Domain/Order.cs
- src/Services/Order/PosCafe.Order.Infrastructure/OrderCommandService.cs
- src/Services/Order/PosCafe.Order.Infrastructure/Persistence/OrderDbContext.cs
- src/Services/Order/PosCafe.Order.Infrastructure/Messaging/OrderOutboxPublisher.cs

---

# Phase 1: Nghiệp Vụ POS Và DDD

## 1.1. Bounded context là gì?

Bounded context là ranh giới trong đó một từ và một quy tắc có ý nghĩa thống nhất. PosCafe có các context:

- Identity: ai là người dùng, role gì, được truy cập store nào.
- Store: cửa hàng và trạng thái hoạt động.
- Catalog: category, product, giá và trạng thái sản phẩm.
- Order: order, line, trạng thái order và tổng tiền.
- Payment: payment, authorize, refund và payment projection của Order.
- Inventory: tồn kho, reserve, release và adjust.
- Kitchen: trạng thái xử lý món trong bếp.
- Reporting: dữ liệu đọc cho báo cáo.

Không nên dùng một model dùng chung cho toàn hệ thống chỉ vì các service cùng viết C#. Shared model thường làm các boundary bị dính vào nhau. BuildingBlocks chỉ nên chứa technical cross-cutting hoặc abstraction thật sự dùng chung.

## 1.2. Aggregate và invariant

Aggregate là cụm object được thay đổi như một đơn vị. Aggregate root là cửa vào duy nhất và là transaction boundary.

Order aggregate tại src/Services/Order/PosCafe.Order.Domain/Order.cs có các invariant:

- StoreId không được rỗng.
- Channel không được rỗng.
- Order mới ở trạng thái Draft.
- Chỉ Draft mới được AddLine.
- Chỉ Draft mới được Confirm.
- Confirm cần ít nhất một line.
- Order Completed hoặc Cancelled không thể Cancel tiếp.

Đoạn domain rút gọn:

~~~csharp
public static Order Create(Guid storeId, string channel)
{
    if (storeId == Guid.Empty)
        throw new ValidationException("Store is required.");

    if (string.IsNullOrWhiteSpace(channel))
        throw new ValidationException("Channel is required.");

    var order = new Order(Guid.NewGuid(), storeId, channel.Trim());
    order.Raise(new OrderCreatedDomainEvent(
        order.Id, order.StoreId, order.CreatedAtUtc));

    return order;
}

public void Confirm()
{
    EnsureDraft();

    if (_lines.Count == 0)
        throw new ValidationException(
            "An order must contain at least one line.");

    Status = OrderStatus.Confirmed;
    ConfirmedAtUtc = DateTimeOffset.UtcNow;

    Raise(new OrderConfirmedDomainEvent(
        Id, StoreId, Subtotal, ConfirmedAtUtc.Value,
        _lines.Select(x => new OrderLineSnapshot(
            x.ProductId, x.Quantity)).ToArray()));
}
~~~

Tại sao invariant phải nằm trong Domain thay vì controller?

Nếu chỉ validate ở controller, một đường gọi khác như background job, test, migration script hoặc handler có thể bypass rule. Domain phải tự bảo vệ mình vì mọi caller đều có thể sai.

## 1.3. Domain event và integration event

- Domain event: sự kiện trong bounded context, thường được aggregate raise.
- Integration event: message contract gửi qua boundary tới service khác.

Ví dụ OrderConfirmedDomainEvent được tạo trong Domain. OrderCommandService chuyển event thành OutboxMessage. Publisher đưa nó vào Kafka với event-type là OrderConfirmed.v1.

Đây là sự phân tách quan trọng:

~~~text
Domain object -> Domain event -> Outbox record -> Kafka integration message
~~~

Không publish Kafka trực tiếp bên trong aggregate. Domain không nên biết Kafka tồn tại.

## 1.4. Order lifecycle

Trạng thái khái niệm:

~~~mermaid
stateDiagram-v2
    [*] --> Draft
    Draft --> Confirmed: Confirm
    Draft --> Cancelled: Cancel
    Confirmed --> Completed: Business completion
    Confirmed --> Cancelled: Cancel/compensation
    Completed --> [*]
    Cancelled --> [*]
~~~

Flow nghiệp vụ thực tế:

1. Cashier/client gửi CreateOrder.
2. Order API xác thực JWT và store scope.
3. Domain tạo Order Draft và các OrderLine.
4. Client hoặc operator confirm Order.
5. Order tạo OrderConfirmed domain event.
6. Order transaction ghi Order, audit và outbox.
7. Publisher gửi event đến pos.order.events.
8. Payment, Inventory và Reporting đọc cùng event bằng các consumer group riêng.
9. Các service cập nhật state của chính mình.
10. Client đọc state qua từng API/read model; không giả định mọi service cập nhật đồng thời.

---

# Phase 2: CQRS, API, Exception Và Idempotency

## 2.1. CQRS

CQRS là tách:

- Command: thay đổi state, cần invariant, authorization và transaction.
- Query: đọc dữ liệu, ưu tiên projection/read model và không tạo side effect.

Ví dụ:

| Thao tác | Loại |
|---|---|
| POST /api/v1/orders | Command |
| POST /orders/{id}/confirm | Command |
| POST /payments/{id}/authorize | Command |
| GET /api/v1/orders/{id} | Query |
| GET /api/v1/reports/daily-sales/... | Query |

CQRS không bắt buộc phải có hai database ngay lập tức. Điểm đầu tiên của CQRS là tách ý định và trách nhiệm; sau đó mới tối ưu read model.

## 2.2. Authorization theo role và store

Authentication trả lời “user là ai?”. Authorization trả lời “user được làm gì?”.

AuthenticationExtensions.cs khai báo policy như:

~~~csharp
options.AddPolicy("order-operator",
    policy => policy.RequireRole("admin", "manager", "cashier"));

options.AddPolicy("inventory-manager",
    policy => policy.RequireRole(
        "admin", "store-manager", "inventory-manager"));
~~~

Ngoài role, StoreAuthorization.cs kiểm tra user có truy cập StoreId trong claim/assignment hay không.

Một lỗi bảo mật phổ biến là chỉ kiểm tra role mà quên tenant/store scope. Trong POS, cashier của Store A không mặc nhiên được đọc Order của Store B.

## 2.3. Exception mapping

Domain/Application không nên trả HTTP response trực tiếp. Nó ném exception có ý nghĩa; middleware chuyển thành API contract.

~~~csharp
var (status, title) = exception switch
{
    ValidationException => (400, "Validation failed"),
    UnauthorizedException => (401, "Unauthorized"),
    ForbiddenException => (403, "Forbidden"),
    NotFoundException => (404, "Resource not found"),
    ConflictException => (409, "Conflict"),
    DomainException => (422, "Business rule violated"),
    _ => (500, "An unexpected error occurred")
};
~~~

Code thực tế: BuildingBlocks/BuildingBlocks/Exceptions/ và src/PosCafe.ServiceDefaults/ExceptionHandlingMiddleware.cs.

Nguyên tắc production:

- Client nhận status/code ổn định.
- Không gửi stack trace hoặc connection string.
- Log exception ở server.
- Response có trace/correlation identifier để support tìm log.
- Lỗi kỹ thuật không được ngụy trang thành lỗi nghiệp vụ.

## 2.4. Idempotency là gì?

Network retry là chuyện bình thường:

1. Client gửi request.
2. Server ghi DB thành công.
3. Response bị mất trên đường truyền.
4. Client gửi lại.

Nếu không có idempotency, có thể tạo hai Order hoặc hai Payment.

Client gửi:

~~~http
POST /api/v1/orders
Idempotency-Key: cashier-20260831-order-00042
Content-Type: application/json
~~~

Server lưu:

- IdempotencyKey.
- RequestHash.
- ResourceId.
- Status/response cần replay.
- CreatedAtUtc.

Nếu key đã tồn tại:

- Hash giống: trả kết quả cũ và đặt Idempotency-Replayed: true.
- Hash khác: trả 409 Conflict.
- Key mới: xử lý và lưu record cùng transaction.

Idempotency record phải có unique index. Cách này chống cả retry tuần tự và race giữa hai request đồng thời. Retention không được ngắn hơn thời gian client có thể retry.

---

# Phase 3: Kafka Và Event-Driven Architecture

## 3.1. Kafka bằng ví dụ dễ hiểu

Hãy tưởng tượng Kafka là một cuốn nhật ký phân tán:

- Topic là một cuốn nhật ký theo loại sự kiện.
- Partition là các quyển con trong cùng topic.
- Message là một dòng trong nhật ký.
- Offset là số dòng.
- Key quyết định message vào partition nào.
- Consumer group là một nhóm người đọc chia nhau các partition.
- Kafka giữ message theo retention, không xóa chỉ vì một consumer đã đọc.

Các topic hiện có trong config:

~~~json
{
  "Messaging": {
    "RequiredTopics": [
      "pos.order.events",
      "pos.payment.events",
      "pos.payment.order-events.dlq"
    ],
    "MinimumTopicPartitions": 1
  },
  "Outbox": {
    "Payment": {
      "Topic": "pos.payment.events",
      "InputTopic": "pos.order.events",
      "ConsumerGroup": "pos-payment-order-events-v1",
      "DeadLetterTopic": "pos.payment.order-events.dlq"
    }
  }
}
~~~

File nguồn: src/Services/Payment/PosCafe.Payment.Api/appsettings.json.

## 3.2. Partition và ordering

Kafka chỉ đảm bảo thứ tự trong cùng một partition. Để các event của một Order giữ thứ tự, publisher dùng AggregateId làm key:

~~~csharp
new Message<string, string>
{
    Key = message.AggregateId,
    Value = message.Payload,
    Headers = headers
}
~~~

Nếu key là OrderId, các event của cùng order thường vào cùng partition. Tuy nhiên:

- Không có ordering toàn topic.
- Tăng partition có thể thay đổi phân phối key.
- Hai aggregate khác nhau có thể xử lý song song.
- Consumer vẫn phải chịu duplicate và retry.

## 3.3. Consumer group

Payment và Inventory đều đọc pos.order.events nhưng dùng group khác nhau:

~~~text
pos-payment-order-events-v1
pos-inventory-order-events-v1
pos-reporting-order-events-v1
~~~

Kết quả:

- Payment nhận một bản.
- Inventory nhận một bản.
- Reporting nhận một bản.
- Hai instance Payment cùng group chia partition, không xử lý trùng do scale-out bình thường.

Nếu vô tình đổi group id, consumer có thể đọc lại từ AutoOffsetReset=Earliest. Đổi group là một quyết định vận hành, không phải thay string tùy ý.

## 3.4. Message headers

Publisher của Order gắn metadata:

~~~csharp
new Headers
{
    { "event-type", Encoding.UTF8.GetBytes(message.EventType) },
    { "event-id", Encoding.UTF8.GetBytes(message.Id.ToString()) },
    { "correlation-id",
      Encoding.UTF8.GetBytes(message.CorrelationId
          ?? message.Id.ToString("N")) },
    { "causation-id",
      Encoding.UTF8.GetBytes(message.Id.ToString()) },
    { "traceparent",
      Encoding.UTF8.GetBytes(activity?.Id ?? string.Empty) },
    { "schema-version", Encoding.UTF8.GetBytes("1") },
    { "schema-id",
      Encoding.UTF8.GetBytes(schemaId) }
}
~~~

Header dùng cho metadata; payload dùng cho business data. Consumer không nên tin mù payload mà bỏ qua event id/schema header.

## 3.5. Correlation ID, causation ID và traceparent

### Correlation ID lấy từ đâu?

Boundary đầu tiên của flow nên:

1. Nhận W3C trace context hoặc X-Correlation-Id do caller gửi.
2. Validate chiều dài/format nếu hệ thống có policy.
3. Nếu không có, tạo ID mới.
4. Gắn vào log scope, audit entry và outgoing message.

Trong code hiện tại, Order/Payment lấy:

~~~csharp
var correlationId =
    Activity.Current?.TraceId.ToString()
    ?? Guid.NewGuid().ToString("N");
~~~

Đây là một implementation đơn giản và có giá trị thực tế. Khi triển khai một hệ thống lớn, nên thống nhất rõ:

- Correlation ID nghiệp vụ có thể sống qua nhiều message.
- Trace ID là ID của distributed trace.
- Không dùng raw request body hoặc user email làm correlation.
- Không ghi correlation ID vào metric label nếu cardinality cao.

### Causation ID

Nếu OrderConfirmed gây ra PaymentCreated, PaymentCreated có:

- correlation-id: cùng business flow với Order.
- causation-id: event OrderConfirmed trực tiếp gây ra nó.

Nhờ vậy có thể phân biệt “cùng một flow” và “nguyên nhân trực tiếp”.

### traceparent

traceparent là W3C context dành cho tracing. Consumer Reporting đọc header này:

~~~csharp
var traceparent = message.Message.Headers
    .FirstOrDefault(x => x.Key == "traceparent") is { } header
    ? Encoding.UTF8.GetString(header.GetValueBytes())
    : null;

ActivityContext.TryParse(
    traceparent, null, true, out var parentContext);

using var activity =
    MessagingTelemetry.ActivitySource.StartActivity(
        "messaging.process",
        ActivityKind.Consumer,
        parentContext);
~~~

Khi debug:

- Tìm business flow bằng correlation-id.
- Tìm timeline kỹ thuật bằng trace-id.
- Tìm message cụ thể bằng event-id.
- Tìm vị trí Kafka bằng topic/partition/offset.

## 3.6. Outbox: giải quyết dual-write

### Vấn đề

Nếu code làm:

~~~text
1. Save Order vào PostgreSQL
2. Publish Kafka
~~~

thì có hai failure window:

- Bước 1 thành công, bước 2 fail: Order có nhưng downstream không biết.
- Bước 2 thành công, bước 1 rollback: downstream biết một Order không tồn tại.

### Cách Outbox hoạt động

OrderCommandService thực hiện:

~~~csharp
await using var transaction =
    await db.Database.BeginTransactionAsync(token);

var order = await action();

var correlationId =
    Activity.Current?.TraceId.ToString()
    ?? Guid.NewGuid().ToString("N");

db.AuditEntries.Add(new AuditEntry
{
    Action = auditAction,
    EntityType = "Order",
    EntityId = order.Id.ToString(),
    StoreId = order.StoreId,
    CorrelationId = correlationId,
    OccurredAtUtc = DateTime.UtcNow
});

foreach (var domainEvent in order.DequeueDomainEvents())
{
    db.OutboxMessages.Add(ToOutbox(
        order, domainEvent, correlationId));
}

await db.SaveChangesAsync(token);
await transaction.CommitAsync(token);
~~~

Cùng transaction ghi:

- orders/order_lines.
- audit_entries.
- outbox_messages.
- idempotency record nếu là command có idempotency.

Nếu commit thành công, outbox chắc chắn tồn tại cùng business state.

### Publisher làm gì?

OrderOutboxPublisher:

1. Mở transaction claim.
2. Chọn message chưa processed, chưa dead-lettered.
3. Dùng FOR UPDATE SKIP LOCKED để nhiều replica không claim cùng record.
4. Gắn LockedUntilUtc và tăng Attempts.
5. Commit claim transaction.
6. Publish từng message ra Kafka.
7. Thành công: ProcessedOnUtc.
8. Thất bại: giữ Error, lease/backoff.
9. Vượt MaxAttempts: gửi DLQ hoặc đánh dấu dead-lettered.

SQL claim rút gọn:

~~~sql
SELECT ...
FROM outbox_messages
WHERE ProcessedOnUtc IS NULL
  AND DeadLetteredOnUtc IS NULL
  AND Attempts < @maxAttempts
  AND (LockedUntilUtc IS NULL OR LockedUntilUtc < @now)
ORDER BY OccurredOnUtc
LIMIT @batchSize
FOR UPDATE SKIP LOCKED;
~~~

OutboxMessage tại BuildingBlocks/BuildingBlocks/Messaging/OutboxMessage.cs có:

- Id.
- EventType.
- AggregateId.
- Payload.
- OccurredOnUtc.
- CorrelationId.
- Attempts.
- ProcessedOnUtc.
- LockedUntilUtc.
- DeadLetteredOnUtc.
- Error.

### Outbox không phải exactly-once

Publisher có thể:

1. Publish Kafka thành công.
2. Process bị crash trước khi mark ProcessedOnUtc.
3. Lần sau publish lại.

Vì vậy semantics thực tế là at-least-once. Consumer phải idempotent.

## 3.7. Inbox: chống xử lý message trùng

Payment consumer có thể crash sau khi cập nhật projection nhưng trước khi commit Kafka offset. Lần sau Kafka giao lại message.

InboxMessage lưu khóa:

~~~text
(EventId, Consumer)
~~~

Payment dùng consumer name:

~~~text
payment.order-events.v1
~~~

Flow thực tế:

~~~csharp
attempt = await InboxProcessor.RegisterAttemptAsync(
    db,
    eventId,
    "payment.order-events.v1",
    stoppingToken);

await using var transaction =
    await db.Database.BeginTransactionAsync(stoppingToken);

await new OrderEventHandler().HandleAsync(
    db,
    eventId,
    "payment.order-events.v1",
    eventType,
    result.Message.Value,
    stoppingToken);

await transaction.CommitAsync(stoppingToken);
consumer.Commit(result);
~~~

OrderEventHandler gọi TryStartAsync:

~~~csharp
if (!await InboxProcessor.TryStartAsync(
        db, eventId, consumer, cancellationToken))
{
    MessagingMetrics.DuplicateEvents.Add(
        1,
        new KeyValuePair<string, object?>(
            "service", "payment"));
    return false;
}
~~~

Ý nghĩa:

- Chưa có Inbox record: tạo record, xử lý.
- Đã processed: bỏ qua duplicate.
- Đang chưa processed: cho phép retry.
- Sau side effect thành công: MarkProcessedAsync.
- Chỉ commit Kafka offset sau transaction/side effect.

Một lưu ý quan trọng khi học: Inbox chống duplicate ở consumer, nhưng không tự làm business operation idempotent nếu handler tạo side effect ngoài transaction. Mọi external call vẫn cần idempotency key hoặc provider reference riêng.

## 3.8. Retry và backoff

Retry phù hợp với lỗi tạm thời:

- Kafka timeout.
- Database connection tạm gián đoạn.
- Mongo transient error.
- Downstream HTTP timeout.

Retry không phù hợp với lỗi dữ liệu vĩnh viễn:

- JSON sai.
- Schema version không hỗ trợ.
- Product/stock không tồn tại do dữ liệu sai.
- Authorization/contract violation.

Consumer dùng exponential backoff:

~~~csharp
var delaySeconds = Math.Min(
    Math.Max(1, settings.RetryMaxSeconds),
    Math.Max(1, settings.ConsumerRetrySeconds)
        * Math.Pow(2, Math.Min(Math.Max(attempt - 1, 0), 6)));

await Task.Delay(
    TimeSpan.FromSeconds(delaySeconds),
    stoppingToken);
~~~

Cần theo dõi MaxPollIntervalMs. Nếu consumer ngủ lâu hơn khoảng Kafka cho phép, group rebalance có thể xảy ra.

## 3.9. DLQ và replay

DLQ là nơi cô lập message không thể xử lý sau số lần retry. Message DLQ nên giữ:

- Payload gốc.
- Event id.
- Event type/schema.
- Original topic.
- Original partition.
- Original offset.
- Reason.
- Consumer.
- Attempt count.
- Correlation/trace headers.

DLQ không phải thùng rác. Quy trình xử lý:

1. Alert báo DLQ tăng.
2. Operator xem event id và reason.
3. Xác định lỗi code, dữ liệu hay schema.
4. Sửa/deploy consumer tương thích.
5. Replay event cụ thể.
6. Theo dõi consumer lag và side effect.
7. Audit kết quả replay.

Gateway có DLQ management tại:

- src/Gateway/PosCafe.ApiGateway/DlqManagementService.cs
- src/Gateway/PosCafe.ApiGateway/Program.cs
- ops/dlq-replay.md

Replay hiện có authorization, Idempotency-Key, lease/fencing, history trong opsdb và audit.

## 3.10. Saga choreography

Saga là business transaction kéo dài qua nhiều service. Choreography nghĩa là không có coordinator trung tâm; service lắng nghe event và phát event tiếp theo.

Flow khái niệm:

~~~mermaid
sequenceDiagram
    participant O as Order
    participant K as Kafka
    participant P as Payment
    participant I as Inventory
    participant R as Reporting

    O->>O: Confirm aggregate
    O->>K: OrderConfirmed
    K->>P: Consume in payment group
    K->>I: Consume in inventory group
    K->>R: Consume in reporting group
    P->>P: Create/update payment projection
    I->>I: Reserve stock
    R->>R: Upsert daily sales
~~~

Failure không rollback như database transaction:

~~~mermaid
flowchart TD
    OC[OrderConfirmed] --> P[Payment]
    OC --> I[Inventory]
    P -->|success| PA[Payment authorized]
    I -->|success| IR[Inventory reserved]
    P -->|failure| PF[Payment failed]
    I -->|failure| IF[Stock unavailable]
    PF --> RC[Refund/cancel compensation]
    IF --> RI[Release/cancel compensation]
    RC --> E[Audit + alert]
    RI --> E
~~~

Repository hiện có choreography và consumer/projection; chưa có Saga Orchestrator hoặc persistent Saga state machine riêng. Đây là giới hạn phải biết khi đọc code. Nếu sau này flow cần timeout, status tổng hợp, manual intervention hoặc compensation nhiều bước, cần cân nhắc orchestrator.

Compensation không phải “undo SQL”. Ví dụ:

- Payment đã authorize nhưng Inventory fail -> refund payment.
- Inventory đã reserve nhưng Payment fail -> release stock.
- Refund cũng fail -> giữ trạng thái cần operator xử lý, không retry vô hạn mù.

---

# Phase 4: Data, PostgreSQL, MongoDB Và Consistency

## 4.1. PostgreSQL là source of truth cho transaction

Các service transactional có DbContext và migration riêng:

- OrderDbContext.
- PaymentDbContext.
- InventoryDbContext.
- CatalogDbContext.
- StoreDbContext.
- IdentityDbContext.
- OpsDbContext.

OrderDbContext có các bảng logic:

~~~text
orders
order_lines
outbox_messages
inbox_messages
audit_entries
order_idempotency_records
~~~

Mỗi service sở hữu database của mình. Không join trực tiếp bảng Order từ Payment. Payment nhận Order event và tạo PaymentOrderProjection.

## 4.2. Payment projection

Payment cần biết StoreId/Total của Order nhưng không query database Order. Payment consumer nhận OrderConfirmed và ghi projection:

~~~csharp
var projection = await db.OrderProjections
    .SingleOrDefaultAsync(
        x => x.OrderId == confirmed.OrderId,
        cancellationToken);

if (projection is null)
{
    db.OrderProjections.Add(new PaymentOrderProjection
    {
        OrderId = confirmed.OrderId,
        StoreId = confirmed.StoreId,
        Total = confirmed.Total,
        UpdatedAtUtc = DateTime.UtcNow
    });
}
else
{
    projection.StoreId = confirmed.StoreId;
    projection.Total = confirmed.Total;
    projection.UpdatedAtUtc = DateTime.UtcNow;
}
~~~

Do đó ngay sau khi tạo Order, gọi Create Payment có thể nhận “Order projection is not available yet.” Đây là eventual consistency, không nhất thiết là lỗi hệ thống.

## 4.3. MongoDB cho Reporting

Reporting có:

- daily_sales collection.
- processed_reporting_events collection.

MongoReportingRepository tạo index:

~~~csharp
var keys = Builders<DailySalesReadModel>
    .IndexKeys
    .Ascending(x => x.StoreId)
    .Ascending(x => x.BusinessDate);

await collection.Indexes.CreateOneAsync(
    new CreateIndexModel<DailySalesReadModel>(
        keys,
        new CreateIndexOptions
        {
            Unique = true,
            Name = "ux_daily_sales_store_date"
        }));
~~~

Khi nhận OrderConfirmed, Reporting dùng Mongo transaction để:

1. Insert event id vào processed events.
2. Increment GrossSales.
3. Increment OrderCount.
4. Commit cả hai side effect.

~~~csharp
session.StartTransaction();

await events.InsertOneAsync(
    session,
    new ProcessedReportingEvent(eventId, DateTime.UtcNow),
    cancellationToken: ct);

var update = Builders<DailySalesReadModel>.Update
    .Inc(x => x.GrossSales, total)
    .Inc(x => x.OrderCount, 1)
    .Set(x => x.UpdatedAtUtc, DateTime.UtcNow)
    .SetOnInsert(x => x.StoreId, storeId)
    .SetOnInsert(x => x.BusinessDate, businessDate);

await collection.UpdateOneAsync(
    session, filter, update,
    new UpdateOptions { IsUpsert = true },
    ct);

await session.CommitTransactionAsync(ct);
~~~

Unique event index và transaction giúp duplicate event không cộng doanh thu hai lần.

## 4.4. Projection lag

Projection lag là khoảng cách giữa event đã có trong Kafka và read model đã cập nhật.

Các trạng thái cần phân biệt:

- Order chưa publish: Outbox backlog.
- Đã publish nhưng consumer chưa đọc: consumer lag.
- Consumer đọc nhưng xử lý fail: retry/DLQ.
- Consumer xử lý nhưng query chưa thấy: Mongo/index/replication issue.
- Query sai store/date: bug contract hoặc authorization.

Không nên “fix” bằng cách query chéo sang write database nếu mục tiêu là read model. Trước hết phải đo lag và hiểu consistency contract.

---

# Phase 5: Audit, Security Và Observability

## 5.1. Audit khác với log

Log trả lời “chương trình đã làm gì?”. Audit trả lời “nghiệp vụ nào đã xảy ra, ai làm, trên entity nào?”.

AuditEntry có các trường quan trọng:

- Action.
- EntityType.
- EntityId.
- ActorId.
- StoreId.
- CorrelationId.
- OccurredAtUtc.
- MetadataJson nếu cần.

Ví dụ Order ghi audit cùng transaction với business state và outbox. Nếu Order commit thành công, audit cũng tồn tại; nếu rollback, không có audit giả.

Log có thể chứa stack trace để debug. Audit cần retention, archive, quyền truy cập và không được ghi secrets.

## 5.2. Retention và archive

Retention worker xóa record cũ theo batch. Với audit có archive option:

- Nếu archive bật, archive thành công trước.
- Chỉ purge sau khi archive hoàn tất.
- Archive failure phải làm alert.
- Không purge trước rồi mới cố archive.

Idempotency retention và audit retention là hai chính sách khác nhau. Không đặt idempotency retention quá ngắn so với retry window của client.

## 5.3. JWT và secret

Identity phát JWT; Gateway và service validate signing key. Ngoài Development:

~~~csharp
if (string.IsNullOrWhiteSpace(jwtKey)
    || Encoding.UTF8.GetByteCount(jwtKey) < 32)
{
    throw new InvalidOperationException(
        "Jwt:Key must be configured with at least 32 bytes.");
}
~~~

Production rules:

- JWT key qua secret manager.
- Không commit appsettings secret.
- Rotate key có kế hoạch overlap hoặc key id nếu hệ thống hỗ trợ.
- Không log Authorization header.
- Internal Reporting API dùng key riêng, constant-time comparison.
- Database/Kafka dùng TLS và credentials secret-managed.

## 5.4. Health check

Có hai câu hỏi khác nhau:

- Readiness: instance có nhận traffic được không?
- Liveness: process còn sống không?

ServiceDefaults map:

~~~csharp
app.MapHealthChecks("/health").AllowAnonymous();

app.MapHealthChecks("/alive",
    new HealthCheckOptions
    {
        Predicate = r => r.Tags.Contains("live")
    }).AllowAnonymous();
~~~

Các health check quan trọng:

- Database check.
- Kafka connectivity.
- Required topic metadata.
- Consumer lag.
- MongoDB.
- Archive/retention freshness.

Không dùng dependency outage làm liveness nếu không muốn restart storm. Dependency outage thường làm readiness fail, process vẫn sống để retry.

## 5.5. Metrics, logs và traces

OpenTelemetry trong ServiceDefaults thêm:

- ASP.NET Core instrumentation.
- HttpClient instrumentation.
- Runtime instrumentation.
- PosCafe.Messaging meter.
- PosCafe.Messaging ActivitySource.
- OTLP exporter nếu có endpoint.
- Prometheus exporter khi bật config.

Metrics nên có:

- Request count/error/latency.
- Outbox pending/backlog.
- Publish failures.
- Consumer lag.
- Retry count.
- DLQ count.
- Idempotency replay/conflict.
- Projection lag.
- Audit/retention failure.

Không đặt raw order id, email hoặc idempotency key vào metric label vì cardinality sẽ bùng nổ. Các ID đó phù hợp cho log/tracing, không phù hợp làm label phổ biến.

## 5.6. Rate limiting

ServiceDefaults có global fixed-window limiter theo remote IP và policy riêng cho DLQ operations. DLQ là operation nhạy cảm nên cần:

- Role authorization.
- Topic scope.
- Rate limit.
- Idempotency.
- Audit.
- Replay history.
- Lease/fencing để hai operator không replay cùng record.

---

# Phase 6: Tư Duy Deploy Production

## 6.1. Deployment không phải chỉ là chạy container

Một deployment hoàn chỉnh phải trả lời:

1. Artifact nào được chạy?
2. Config/secret lấy từ đâu?
3. Database schema version nào?
4. Topic nào đã tồn tại, partition bao nhiêu?
5. Dependency nào ready trước API?
6. Khi rollout fail, traffic đi đâu?
7. Khi rollback code, schema có tương thích không?
8. Theo dõi metric nào trước/trong/sau deploy?
9. Backup và khôi phục thế nào?
10. Ai có quyền replay/migrate/rotate secret?

## 6.2. Artifact bất biến

deploy/Dockerfile dùng multi-stage:

~~~dockerfile
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG PROJECT
WORKDIR /src
COPY . .
RUN dotnet restore PosCafe.slnx
RUN dotnet publish "${PROJECT}" -c Release     --no-restore -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["sh", "-c", "dotnet ${APP_DLL}"]
~~~

Tư duy cần có:

- Build một lần.
- Tag image bằng commit SHA hoặc release version.
- Promote cùng image qua staging/production.
- Không rebuild khác nội dung giữa môi trường.
- Config inject lúc runtime.
- Không dùng tag mutable latest cho production rollout.

## 6.3. Dependency graph

### Local Aspire

AppHost dùng WaitFor để process local không start quá sớm.

### Docker Compose

Compose production có:

1. PostgreSQL.
2. MongoDB replica set.
3. Mongo init job.
4. Kafka/Redpanda.
5. Kafka topic init job.
6. PostgreSQL migrator.
7. API services.
8. Gateway.

Flow:

~~~mermaid
flowchart TD
    IMG[Build images] --> PG[PostgreSQL healthy]
    IMG --> MG[Mongo healthy]
    IMG --> KF[Kafka healthy]
    MG --> MI[Mongo replica init]
    KF --> KT[Create required topics]
    PG --> DB[Migration job]
    MI --> REP[Reporting/Catalog may start]
    KT --> MSG[Kafka services may start]
    DB --> API[Database API replicas]
    MSG --> API
    API --> RD[Readiness healthy]
    RD --> ING[Ingress/Gateway traffic]
~~~

Một init job phải idempotent:

- Mongo init kiểm tra replica status trước initiate.
- Kafka init describe trước create.
- Migration dùng EF history, không tự viết “create table if missing” ngoài migration.

## 6.4. Database migration

Các API ngoài Development không tự migrate khi start. Lý do:

- Hai replica cùng migrate.
- Migration lock kéo dài làm health fail.
- Rollout không còn deterministic.
- API startup phụ thuộc quyền DDL không cần thiết.

Migration.Dockerfile build solution và chạy deploy/migrate.sh. Script lần lượt update:

~~~text
Identity
Store
Catalog
Order
Payment
Inventory
Gateway operations
~~~

Production release flow:

~~~mermaid
sequenceDiagram
    participant CI as CI/CD
    participant REG as Image registry
    participant DB as Database
    participant JOB as Migration job
    participant API as API replicas
    participant ING as Ingress

    CI->>REG: Push immutable API images
    CI->>REG: Push migration image
    CI->>DB: Backup/check migration window
    CI->>JOB: Run migration with short-lived identity
    JOB->>DB: Apply EF migrations
    DB-->>JOB: Success
    CI->>API: Rolling update
    API->>API: Readiness checks
    API-->>ING: Ready
    ING->>API: Send traffic
~~~

### Expand/contract

Không deploy breaking schema trong một bước:

1. Expand: thêm cột/table nullable hoặc backward-compatible.
2. Deploy code mới đọc được schema cũ và mới.
3. Backfill theo batch.
4. Chuyển đọc/ghi.
5. Contract ở release sau: xóa field/table cũ.

Rollback code không phải rollback migration. Nếu schema đã breaking, dùng forward fix hoặc contract plan.

## 6.5. Kubernetes, Helm và Kustomize

Repository có hai hướng:

- deploy/k8s/: baseline manifest/Kustomize.
- deploy/helm/poscafe/: chart parameter hóa image, replica, resource, PDB, HPA, ingress, ExternalSecret và migration hook.

Kubernetes production checklist:

- Namespace có Pod Security restricted.
- Workload chạy non-root.
- seccomp RuntimeDefault.
- Drop capabilities.
- read-only filesystem nếu ứng dụng hỗ trợ.
- Service name khớp Gateway YARP cluster destination.
- Readiness/liveness có path và port đúng.
- Secret không nằm trong repository.
- NetworkPolicy deny-by-default.
- PDB không thấp hơn khả năng chịu lỗi.
- HPA chỉ bật khi resource request và metrics-server sẵn sàng.

Service names phải khớp YARP:

~~~json
"Clusters": {
  "order": {
    "Destinations": {
      "primary": { "Address": "http://order" }
    }
  }
}
~~~

Nếu Kubernetes Service tên poscafe-order nhưng YARP gọi order, Gateway sẽ 502 dù cả hai Pod đều Running.

## 6.6. Security boundary khi deploy

Public:

- Ingress/TLS.
- Gateway public routes.
- Authentication/authorization.

Private:

- API nội bộ.
- PostgreSQL.
- MongoDB.
- Kafka.
- Health.
- Metrics.
- Migration job.
- DLQ operations nếu có thể giới hạn qua private admin network.

Production thật nên dùng:

- Managed PostgreSQL HA.
- Managed MongoDB/replica set.
- Managed Kafka hoặc cluster vận hành đúng replication/ACL.
- Secret manager/External Secrets.
- Centralized logs and traces.
- Backup/restore đã được diễn tập.

Docker Compose trong repository là self-hosted baseline, không tự biến thành HA chỉ vì chạy nhiều container.

## 6.7. Rollout và graceful shutdown

Rolling update an toàn cần:

- maxUnavailable phù hợp.
- Readiness pass trước nhận traffic.
- Liveness không phụ thuộc nhầm vào database.
- Graceful shutdown cho HTTP request.
- Publisher flush/close Kafka.
- Consumer stop polling và close group.
- Connection pool dispose.
- PDB để không mất toàn bộ replica trong maintenance.

Nếu readiness fail sau rollout:

1. Dừng tăng rollout.
2. Xem logs và dependency health.
3. So sánh config/image/schema.
4. Nếu code tương thích, rollback image.
5. Nếu migration đã breaking, không rollback mù; forward-fix.

## 6.8. Observability-driven deploy

### Trước deploy

- Image digest đúng.
- Config diff đã review.
- Secret tồn tại.
- Kafka topic/partition/ACL đúng.
- Migration SQL/change plan rõ.
- Backup/restore point có thể dùng.
- Alert/dashboard hoạt động.
- Rollback image đã xác định.

### Trong deploy

- Migration job status.
- Pod scheduling.
- Readiness/liveness.
- HTTP error rate và latency.
- Database connections/locks.
- Outbox backlog.
- Kafka publish failures.
- Consumer lag.
- DLQ count.

### Sau deploy

- Business flow tạo/confirm Order.
- Payment projection lag.
- Inventory reserve.
- Reporting daily sales.
- Audit records.
- Idempotency replay/conflict.
- No unexpected 401/403/409/5xx spike.

## 6.9. RPO, RTO và recovery

- RPO: mất dữ liệu tối đa chấp nhận được.
- RTO: thời gian tối đa để hệ thống phục hồi.

Cần phân biệt:

- Backup database không chứa message chưa commit.
- Kafka retention không thay thế backup database.
- Replay event không tự sửa mọi side effect ngoài Kafka.
- Mongo projection có thể rebuild nếu event còn đủ và consumer tương thích.
- DLQ replay cần kiểm tra duplicate/compensation trước.

---

# Phase 7: Debug Một Flow Từ Đầu Đến Cuối

Giả sử OrderId là O và user gửi Create -> Confirm.

## Bước 1: Client và Gateway

Request đi tới:

~~~text
POST /api/v1/orders
Authorization: Bearer <jwt>
Idempotency-Key: request-123
X-Correlation-Id: flow-456
~~~

Gateway:

- Match route orders.
- Validate JWT.
- Forward đến cluster order.
- Ghi request/trace context.

Nếu JWT sai: dừng ở Gateway/API, không có DB transaction.

## Bước 2: Order API

Order API:

- Kiểm tra role order-operator.
- Kiểm tra user có StoreId.
- Validate Idempotency-Key.
- Tính request hash.
- Gọi IOrderCommandService.

Nếu key đã tồn tại với hash giống: trả replay. Không tạo aggregate mới.

## Bước 3: Domain

Order.Create kiểm tra StoreId/channel. AddLine kiểm tra Draft. Confirm kiểm tra có line và raise event.

Nếu invariant fail:

- Không mở/commit business transaction thành công.
- Exception middleware map response.
- Log có trace/correlation.

## Bước 4: PostgreSQL transaction

OrderCommandService ghi trong một transaction:

~~~text
orders
order_lines
audit_entries
outbox_messages
order_idempotency_records
~~~

Nếu commit fail: client nhận lỗi; lần retry cùng key có thể chạy lại an toàn.

## Bước 5: Outbox publisher

Publisher claim message. Nếu Kafka down:

- Outbox vẫn còn.
- Attempts/Error/LockedUntilUtc thay đổi.
- Metrics publish failure tăng.
- Không mất Order.

Nếu Kafka publish success nhưng process crash trước mark processed, message có thể gửi lại. Đây là lý do consumer cần Inbox.

## Bước 6: Payment consumer

Payment đọc event với group payment:

- Validate schema-version.
- Validate event-id.
- Tạo Activity consumer từ traceparent.
- Register Inbox attempt.
- OrderEventHandler TryStart.
- Upsert PaymentOrderProjection.
- Mark Inbox processed.
- Commit transaction.
- Commit Kafka offset.

Nếu projection transaction fail, offset không commit, consumer retry.

## Bước 7: Inventory consumer

Inventory đọc cùng event trong group riêng:

- Validate headers/schema.
- Register attempt.
- Transaction.
- TryStart Inbox.
- Tìm StockItem cho từng line.
- Reserve quantity.
- Save, mark processed, commit offset.

Nếu thiếu stock:

- Không commit offset trong lần retry.
- Sau max attempts, publish inventory DLQ.
- Mark Inbox dead-lettered.
- Operator xử lý hoặc compensation theo business policy.

## Bước 8: Reporting consumer

Reporting:

- Validate OrderConfirmed.v1.
- Register event attempt trong Mongo.
- Mongo transaction insert processed event + increment daily sales.
- Duplicate event bị chặn bởi unique event id.
- Commit Kafka offset sau Mongo commit.

Query báo cáo có thể chưa thấy dữ liệu ngay sau Confirm. Đó là projection lag, cần xem metric trước khi kết luận mất dữ liệu.

## Bước 9: Điều tra bằng ID

Dùng:

- flow-456 để tìm toàn business flow.
- event-id để tìm message cụ thể.
- trace-id để xem spans.
- OrderId để tìm aggregate/business record.
- topic/partition/offset để tìm Kafka location.
- DLQ replay history id để tìm operator action.

---

# Phase 8: Tư Duy Middle Lên Senior

## 8.1. Câu hỏi khi thêm command

1. Aggregate nào sở hữu invariant?
2. Có cần transaction với bảng nào?
3. Request có thể retry không?
4. Idempotency record lưu gì để replay response?
5. Audit action và actor/store scope là gì?
6. Domain event nào cần phát?
7. Event payload có PII/secret không?
8. Downstream nào nghe event?
9. Nếu downstream chậm thì API response nói gì?
10. Retry và DLQ policy là gì?

## 8.2. Câu hỏi khi thêm consumer

1. Event id nằm ở header hay payload?
2. Schema version được validate thế nào?
3. Key Inbox là EventId + Consumer hay gì khác?
4. Side effect và mark processed có cùng transaction không?
5. Commit offset xảy ra sau side effect chưa?
6. Lỗi transient và permanent phân biệt ra sao?
7. MaxPollInterval có đủ cho backoff không?
8. DLQ có giữ original offset và reason không?
9. Replay có thể tạo duplicate side effect không?
10. Có metric lag, retry, duplicate và DLQ không?

## 8.3. Câu hỏi khi thay đổi schema

1. Consumer cũ có đọc được event mới không?
2. Field mới có optional không?
3. Có đổi nghĩa field cũ không?
4. Có tăng schema-id/version không?
5. Cần dual-read/dual-write không?
6. Retention còn đủ để replay event cũ không?
7. Có migration projection không?
8. Có compatibility check trong CI không?

## 8.4. Câu hỏi khi deploy

1. Image digest nào đang chạy?
2. Migration chạy trước hay sau rollout?
3. API cũ có tương thích schema mới không?
4. Topic/partition/ACL đã provision chưa?
5. Readiness kiểm tra đúng dependency chưa?
6. Secret rotation có overlap không?
7. Rollback có an toàn sau migration không?
8. Alert nào sẽ báo rollout lỗi?
9. Ai được quyền migration/DLQ replay?
10. Nếu database mất thì RPO/RTO có đạt không?

Senior không chỉ hỏi “code chạy chưa”. Senior hỏi “khi Kafka chết, request retry, message duplicate, migration fail, projection chậm, secret hết hạn hoặc rollout giữa chừng thì hệ thống giữ được invariant nào và operator biết điều gì?”.

---

# Runbook Tóm Tắt

## Migration fail

1. Dừng rollout.
2. Giữ API version cũ nếu còn tương thích.
3. Đọc migration log, lock và database connectivity.
4. Không xóa EF migration history.
5. Kiểm tra backup trước destructive change.
6. Sửa migration/permission/connection rồi chạy lại job có kiểm soát.

## Kafka unavailable

1. Kiểm tra broker, DNS, TLS, ACL.
2. Kiểm tra readiness và producer error.
3. Xác nhận Order state vẫn commit.
4. Xác nhận Outbox backlog tăng thay vì mất.
5. Không tự tạo topic nếu IaC đang sở hữu topic.
6. Khôi phục broker rồi theo dõi publisher/consumer drain backlog.

## Consumer poison message

1. Lấy event id, schema id, topic/partition/offset.
2. Đọc DLQ reason và attempt.
3. Phân loại bug code, dữ liệu hay contract.
4. Deploy consumer tương thích.
5. Replay từng event bằng quyền operator và key mới.
6. Kiểm tra Inbox/idempotency trước khi xác nhận hoàn tất.

## Reporting chậm

1. Kiểm tra Kafka consumer lag.
2. Kiểm tra Mongo health/index/transaction.
3. Kiểm tra processed event và DLQ.
4. Phân biệt not-yet-projected với missing source event.
5. Sửa consumer rồi replay hoặc rebuild projection.
6. Đối chiếu tổng doanh thu sau recovery.

## Rollback

- Rollback image nếu schema backward-compatible.
- Không rollback mù nếu migration đã breaking.
- Ưu tiên forward fix/expand-contract.
- Theo dõi business KPI, không chỉ Pod Ready.

---

# Glossary

- **DDD**: thiết kế phần mềm quanh nghiệp vụ, ngôn ngữ nghiệp vụ và boundary.
- **Bounded context**: ranh giới nơi model và từ vựng có một ý nghĩa thống nhất.
- **Aggregate**: cụm object có aggregate root bảo vệ invariant.
- **Invariant**: quy tắc luôn phải đúng.
- **CQRS**: tách luồng thay đổi state và luồng đọc state.
- **Domain event**: điều đã xảy ra trong domain.
- **Integration event**: contract gửi qua boundary service.
- **Outbox**: bảng lưu event cùng transaction business trước khi publish.
- **Inbox**: bảng ghi trạng thái nhận/xử lý event để chống duplicate.
- **At-least-once**: message có thể được giao lại ít nhất một lần hoặc nhiều lần.
- **Exactly-once**: semantics khó đạt end-to-end; không nên giả định chỉ vì Kafka transaction.
- **Consumer group**: nhóm instance cùng chia partition.
- **Offset**: vị trí message trong partition.
- **Partition**: log tuần tự trong topic.
- **DLQ**: nơi cô lập message không xử lý được.
- **Replay**: đưa lại event đã cô lập vào flow có kiểm soát.
- **Idempotency**: gọi lại cùng request không tạo side effect mới.
- **Saga**: business transaction dài qua service, hoàn tất bằng event/compensation.
- **Choreography**: Saga không có coordinator trung tâm.
- **Compensation**: action nghiệp vụ ngược lại, không phải rollback SQL.
- **Eventual consistency**: các model hội tụ sau một khoảng trễ.
- **Projection**: model/read view được xây dựng từ event.
- **Correlation ID**: mã liên kết một business flow.
- **Causation ID**: event trực tiếp gây ra event hiện tại.
- **Traceparent**: W3C context nối distributed trace.
- **Audit**: record nghiệp vụ có actor/entity/time, khác với debug log.
- **Readiness**: instance có thể nhận traffic hay chưa.
- **Liveness**: process còn sống và phản hồi hay chưa.
- **Observability**: hiểu trạng thái bên trong qua logs, metrics và traces.
- **RPO**: lượng dữ liệu tối đa chấp nhận mất.
- **RTO**: thời gian tối đa chấp nhận để phục hồi.
- **Expand/contract**: chiến lược thay schema qua nhiều release tương thích.

---

# Bản Đồ File Nguồn

## Core và hosting

- src/PosCafe.AppHost/AppHost.cs
- src/PosCafe.ServiceDefaults/Extensions.cs
- src/PosCafe.ServiceDefaults/AuthenticationExtensions.cs
- src/PosCafe.ServiceDefaults/ExceptionHandlingMiddleware.cs
- src/PosCafe.ServiceDefaults/PosCafeRequestMiddleware.cs
- src/PosCafe.ServiceDefaults/StoreAuthorization.cs

## Domain và API

- src/Services/Order/PosCafe.Order.Domain/Order.cs
- src/Services/Order/PosCafe.Order.Domain/OrderLine.cs
- src/Services/Order/PosCafe.Order.Api/Program.cs
- src/Services/Payment/PosCafe.Payment.Api/Program.cs
- src/Services/Inventory/PosCafe.Inventory.Api/Program.cs
- src/Services/Reporting/PosCafe.Reporting.Api/Program.cs

## Persistence và transaction

- src/Services/Order/PosCafe.Order.Infrastructure/OrderCommandService.cs
- src/Services/Order/PosCafe.Order.Infrastructure/Persistence/OrderDbContext.cs
- src/Services/Payment/PosCafe.Payment.Infrastructure/PaymentCommandService.cs
- src/Services/Payment/PosCafe.Payment.Infrastructure/Persistence/PaymentDbContext.cs
- src/Services/Reporting/PosCafe.Reporting.Infrastructure/MongoReportingRepository.cs

## Messaging

- BuildingBlocks/BuildingBlocks/Messaging/OutboxMessage.cs
- BuildingBlocks/BuildingBlocks/Messaging/InboxMessage.cs
- BuildingBlocks/BuildingBlocks/Messaging/InboxProcessor.cs
- BuildingBlocks/BuildingBlocks/Messaging/KafkaProducerConfiguration.cs
- BuildingBlocks/BuildingBlocks/Messaging/KafkaDeadLetter.cs
- BuildingBlocks/BuildingBlocks/Messaging/MessagingMetrics.cs
- src/Services/Order/PosCafe.Order.Infrastructure/Messaging/OrderOutboxPublisher.cs
- src/Services/Payment/PosCafe.Payment.Infrastructure/Messaging/OrderEventsConsumer.cs
- src/Services/Payment/PosCafe.Payment.Infrastructure/Messaging/OrderEventHandler.cs
- src/Services/Inventory/PosCafe.Inventory.Infrastructure/InventoryOrderEventsConsumer.cs
- src/Services/Reporting/PosCafe.Reporting.Infrastructure/ReportingOrderEventsConsumer.cs

## Gateway và operations

- src/Gateway/PosCafe.ApiGateway/appsettings.json
- src/Gateway/PosCafe.ApiGateway/Program.cs
- src/Gateway/PosCafe.ApiGateway/DlqManagementService.cs
- src/Gateway/PosCafe.ApiGateway/DlqReplayHistory.cs
- ops/README.md
- ops/dlq-replay.md
- ops/prometheus/
- ops/grafana/

## Deploy và contract

- deploy/Dockerfile
- deploy/Migration.Dockerfile
- deploy/migrate.sh
- deploy/docker-compose.production.yml
- deploy/helm/poscafe/
- deploy/k8s/
- schemas/order-confirmed.v1.schema.json
- docs/POS-CORE-BUSINESS-AND-TECHNICAL-DESIGN.md


---

# Phụ Lục Thực Chiến: Một Flow Hoàn Chỉnh

Phần này mô phỏng cách một senior đọc và vận hành một yêu cầu thật. Ở mỗi bước cần biết dữ liệu nào được tạo, boundary nào sở hữu dữ liệu, transaction nào đã commit, message nào đang chờ và metric nào chứng minh điều đó.

## A.1. Request tạo Order

### Request vào Gateway

~~~http
POST /api/v1/orders
Authorization: Bearer <jwt>
Idempotency-Key: cashier-20260831-000001
X-Correlation-Id: shift-42-order-001
Content-Type: application/json

{
  "storeId": "11111111-1111-1111-1111-111111111111",
  "channel": "Counter",
  "lines": [
    {
      "productId": "22222222-2222-2222-2222-222222222222",
      "productName": "Cà phê sữa",
      "unitPrice": 35000.00,
      "quantity": 2
    }
  ]
}
~~~

Gateway không tính tổng tiền và không ghi Order. Gateway chỉ xác thực JWT, match route, forward request và giữ trace context. Nếu Gateway trả 401/403, request chưa đi vào Order transaction. Nếu trả 502, kiểm tra YARP destination, DNS Service và readiness của Order trước khi xem database.

### Order API xử lý

~~~csharp
if (!principal.CanAccessStore(command.StoreId))
    return Results.Forbid();

var idempotencyKey =
    request.Headers["Idempotency-Key"].ToString();

var requestHash = Convert.ToHexString(
    SHA256.HashData(
        Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(command))));

var result = await service.CreateAsync(
    command, idempotencyKey, requestHash, ct);
~~~

Điều cần học:

- Authorization có cả role và store scope.
- Hash được tính từ command, không từ header.
- Cùng key nhưng body khác phải là conflict.
- Hash không phải encryption.
- Idempotency key do client tạo, server quyết định semantics.

### Domain thay đổi state

~~~csharp
var order = Order.Create(
    command.StoreId,
    command.Channel);

foreach (var line in command.Lines)
{
    order.AddLine(OrderLine.Create(
        line.ProductId,
        line.ProductName,
        line.UnitPrice,
        line.Quantity));
}

order.Confirm();
~~~

Nếu quantity âm, line rỗng hoặc Order sai trạng thái, exception được ném từ Domain. Domain không nên biết ASP.NET hay HTTP status tồn tại.

### Database sau Create

Sau transaction thành công, dữ liệu khái niệm có thể là:

~~~text
orders
  Id = O
  StoreId = S
  Status = Draft
  Channel = Counter
  Version = 0

order_lines
  OrderId = O
  ProductId = P
  Quantity = 2
  UnitPrice = 35000

audit_entries
  Action = order.created
  EntityId = O
  ActorId = U
  StoreId = S
  CorrelationId = C

order_idempotency_records
  IdempotencyKey = cashier-20260831-000001
  RequestHash = H
  OrderId = O
  Status = Draft
~~~

Create Order chưa nhất thiết phát event xác nhận nếu event chỉ được tạo khi Confirm. Không nhầm “Order đã tạo” với “Order đã sẵn sàng cho Payment”.

## A.2. Request confirm Order

~~~http
POST /api/v1/orders/O/confirm
Authorization: Bearer <jwt>
X-Correlation-Id: shift-42-order-001
~~~

Order API load Order và Lines, kiểm tra tồn tại/store scope, gọi Order.Confirm(), lấy domain event rồi ghi Order mới, audit và outbox trong cùng transaction.

~~~csharp
await using var transaction =
    await db.Database.BeginTransactionAsync(token);

var order = await action();

var correlationId =
    Activity.Current?.TraceId.ToString()
    ?? Guid.NewGuid().ToString("N");

db.AuditEntries.Add(new AuditEntry
{
    Action = "order.confirmed",
    EntityType = "Order",
    EntityId = order.Id.ToString(),
    ActorId = actorId,
    StoreId = order.StoreId,
    CorrelationId = correlationId,
    OccurredAtUtc = DateTime.UtcNow
});

foreach (var domainEvent in
         order.DequeueDomainEvents())
{
    db.OutboxMessages.Add(
        ToOutbox(order, domainEvent, correlationId));
}

await db.SaveChangesAsync(token);
await transaction.CommitAsync(token);
~~~

Thứ tự quan trọng:

- Không gọi producer Kafka trong transaction PostgreSQL.
- Outbox row phải tồn tại trước commit.
- Nếu SaveChanges hoặc Commit fail, không coi business operation là thành công.
- Domain event được chuyển thành integration message ở Infrastructure.

## A.3. Outbox row và Kafka message

Sau commit:

~~~text
Id = E
EventType = OrderConfirmed.v1
AggregateId = O
Payload = { OrderId, StoreId, Total, OccurredAt, Lines }
OccurredOnUtc = T
CorrelationId = C
Attempts = 0
ProcessedOnUtc = null
LockedUntilUtc = null
DeadLetteredOnUtc = null
~~~

Publisher dùng FOR UPDATE SKIP LOCKED:

~~~sql
SELECT ...
FROM outbox_messages
WHERE ProcessedOnUtc IS NULL
  AND DeadLetteredOnUtc IS NULL
  AND Attempts < @maxAttempts
  AND (LockedUntilUtc IS NULL OR LockedUntilUtc < @now)
ORDER BY OccurredOnUtc
LIMIT @batchSize
FOR UPDATE SKIP LOCKED;
~~~

- FOR UPDATE khóa row được claim.
- SKIP LOCKED để replica khác bỏ qua row đang giữ.
- Lease giúp row được claim lại sau crash.
- Claim transaction không giữ database lock trong lúc gọi Kafka.

Message trên pos.order.events:

~~~text
Kafka key: O

Headers:
  event-type = OrderConfirmed.v1
  event-id = E
  correlation-id = C
  causation-id = E
  traceparent = 00-...
  schema-version = 1
  schema-id = order-confirmed.v1

Payload:
{
  "OrderId": "O",
  "StoreId": "S",
  "Total": 70000,
  "OccurredAt": "2026-08-31T12:00:00Z",
  "Lines": [
    { "ProductId": "P", "Quantity": 2 }
  ]
}
~~~

Schema nguồn: schemas/order-confirmed.v1.schema.json.

Nếu publish thành công, publisher set ProcessedOnUtc và xóa lease. Nếu publish thất bại, tăng Attempts, lưu Error, đặt lease/backoff. Nếu process chết sau Kafka publish nhưng trước ProcessedOnUtc, message có thể xuất hiện lại; đó là duplicate hợp lệ trong at-least-once.

## A.4. Payment xử lý event

Payment dùng consumer group pos-payment-order-events-v1:

~~~mermaid
sequenceDiagram
    participant K as Kafka
    participant PC as Payment Consumer
    participant DB as Payment PostgreSQL
    participant I as Inbox
    participant O as Offset

    K->>PC: Consume event E
    PC->>PC: Validate headers/schema
    PC->>DB: RegisterAttempt(E)
    PC->>DB: Begin transaction
    PC->>I: TryStart(E, payment consumer)
    PC->>DB: Upsert PaymentOrderProjection
    PC->>I: MarkProcessed(E)
    PC->>DB: Commit
    PC->>O: Commit Kafka offset
~~~

Các bước thực tế:

1. Validate schema-version và event-id.
2. Tạo consumer Activity từ traceparent.
3. Register attempt trong Inbox.
4. Mở transaction Payment.
5. TryStart để chống duplicate.
6. Upsert PaymentOrderProjection.
7. Mark Inbox processed.
8. Commit database.
9. Commit Kafka offset.

Projection:

~~~text
payment_order_projections
  OrderId = O
  StoreId = S
  Total = 70000
  UpdatedAtUtc = T
~~~

Gọi Create Payment quá sớm có thể nhận “Order projection is not available yet.” Client nên retry có backoff hoặc hiển thị “đang đồng bộ”, không query chéo Order database.

## A.5. Inventory xử lý event

Inventory đọc cùng topic nhưng group khác:

1. Validate event-type, schema-id, schema-version và event-id.
2. Register attempt.
3. Deserialize OrderConfirmed.
4. Mở transaction.
5. TryStart Inbox.
6. Load StockItem theo StoreId + ProductId.
7. Gọi StockItem.Reserve(quantity).
8. Save stock và mark Inbox processed.
9. Commit database.
10. Commit Kafka offset.

Nếu stock không đủ:

~~~text
attempt < max:
  không commit offset
  exponential backoff
  Kafka giao lại message

attempt >= max:
  publish pos.inventory.order-events.dlq
  mark Inbox dead-lettered
  commit source offset
~~~

Reserve nhiều line phải nằm trong cùng transaction Inventory để không reserve một phần rồi fail phần còn lại. Nếu cần gọi external warehouse, không giữ database transaction trong suốt network call; phải thiết kế state và compensation.

## A.6. Reporting xử lý event trong MongoDB

Reporting dùng group pos-reporting-order-events-v1:

~~~mermaid
flowchart LR
    E[OrderConfirmed E] --> V[Validate schema]
    V --> M1[Insert processed event]
    V --> M2[Increment daily_sales]
    M1 --> TX[Mongo transaction]
    M2 --> TX
    TX --> C[Commit]
    C --> O[Commit Kafka offset]
~~~

MongoReportingRepository cập nhật:

- daily_sales.
- processed_reporting_events.

Unique index trên EventId chặn event cộng doanh thu hai lần. Mongo transaction gộp insert processed event và increment daily sales. Nếu Mongo deployment không hỗ trợ transaction, không được giả định code này vẫn an toàn; phải đổi design hoặc capability của cluster.

## A.7. Client retry và idempotency

Response có thể mất sau khi server commit:

~~~mermaid
sequenceDiagram
    participant C as Client
    participant API as Order API
    participant DB as PostgreSQL

    C->>API: Request key K
    API->>DB: Commit Order + idempotency K
    API--xC: Response lost
    C->>API: Retry same key K
    API->>DB: Find K
    DB-->>API: Existing Order O
    API-->>C: Same result + replayed=true
~~~

Nếu request retry cùng key và hash giống, trả kết quả cũ. Nếu body khác, trả 409. Không trả resource cũ cho payload mới. Idempotency record cần unique index và retention dài hơn retry window.

---

# Phụ Lục B: Đọc Failure Bằng Evidence

## B.1. Ma trận lỗi

| Triệu chứng | Tầng nghi ngờ | Evidence cần xem | Không nên làm |
|---|---|---|---|
| 401 | JWT/authentication | Gateway log, key, claims | Mở anonymous toàn API |
| 403 | Role/store scope | claims, StoreAuthorization | Bỏ kiểm tra StoreId |
| 409 | State/idempotency | request hash, record | Tạo key mới để che bug |
| 502 | Gateway/DNS/readiness | YARP, Service, /health | Query DB trước |
| Payment chưa thấy Order | Kafka/projection lag | Outbox, topic, lag | Query chéo Order DB |
| Outbox tăng | Broker/publisher | Error, Attempts, lease | Xóa outbox |
| DLQ tăng | Contract/data/consumer | reason, schema, offset | Replay toàn bộ ngay |
| Reporting chậm | Consumer/Mongo | lag, health, processed events | Sửa số liệu bằng tay |
| Pod restart | Probe/config | logs, env, probe | Tăng timeout mù |
| Migration fail | DB/release job | log, lock, permission | Cho mọi replica tự migrate |

## B.2. Điều tra một OrderId

Với OrderId O và thời điểm T:

1. Tìm Order row và status.
2. Tìm audit theo EntityId O.
3. Tìm outbox theo AggregateId O.
4. Xem ProcessedOnUtc, Attempts và Error.
5. Tìm event id E, topic, partition, offset.
6. Kiểm tra Payment Inbox với (E, payment.order-events.v1).
7. Kiểm tra Inventory Inbox với (E, inventory.order-events.v1).
8. Kiểm tra Reporting processed event E.
9. Kiểm tra PaymentOrderProjection và daily_sales.
10. Đối chiếu correlation/trace trong logs.

Thứ tự này cho biết event chưa tạo, chưa publish, chưa consume, consume fail hay query sai.

## B.3. Log tốt

~~~text
Order:
  order.confirmed order=O event=E correlation=C trace=T

Outbox:
  published service=order topic=pos.order.events event=E key=O

Payment:
  processed order event=E consumer=payment.order-events.v1

Inventory:
  failed order event=E attempt=3 error=insufficient stock

Reporting:
  projection updated event=E store=S businessDate=2026-08-31
~~~

Log nên có field có cấu trúc. Không log Authorization header, payment credential hoặc payload chứa PII.

---

# Phụ Lục C: Tư Duy Release Từng Bước

## C.1. Build và package

~~~mermaid
flowchart LR
    SRC[Source + schema] --> BUILD[dotnet restore/build]
    BUILD --> IMAGE[Immutable API images]
    BUILD --> MIG[Migration image]
    IMAGE --> SCAN[Security/license scan]
    MIG --> SCAN
    SCAN --> STAGE[Deploy staging]
    STAGE --> VERIFY[Readiness + smoke verification]
    VERIFY --> PROD[Promote same digest]
~~~

Artifact cần ghi lại commit SHA, image digest, .NET runtime, migration version, schema version, configuration revision và release actor.

## C.2. Pre-deploy checklist

~~~text
[ ] Image digest đã review
[ ] Config production không có fallback localhost
[ ] JWT/database/Kafka secret tồn tại
[ ] Topic, partition và ACL đúng
[ ] Migration plan đã review
[ ] Backup/restore point đã xác nhận
[ ] Readiness platform truy cập được
[ ] Dashboard/alert hoạt động
[ ] Rollback image đã xác định
[ ] On-call biết release window
~~~

## C.3. Migration release

1. Chặn hoặc cô lập traffic nếu migration breaking.
2. Backup theo RPO.
3. Chạy một migration job với identity có quyền DDL.
4. Chờ exit code thành công.
5. Kiểm tra EF migration history và schema.
6. Chỉ sau đó rollout API.
7. Nếu fail, không tự động tiếp tục rollout.

Migration job nên dùng credential riêng. Runtime API chỉ nên có DML cần thiết; migrator mới cần DDL.

## C.4. Rollout

1. Deploy image digest đã kiểm tra.
2. Pod mới được schedule.
3. Startup và readiness pass.
4. Service thêm endpoint.
5. Ingress chuyển traffic theo rolling strategy.
6. Theo dõi error rate, latency, DB pool và messaging.
7. Kết thúc release sau technical và business smoke flow.

Readiness chỉ chứng minh dependency kỹ thuật sẵn sàng; không chứng minh Payment đã thành công.

## C.5. Rollback decision

~~~mermaid
flowchart TD
    R[Release] --> H{Readiness healthy?}
    H -->|No| STOP[Stop rollout]
    H -->|Yes| E{Error/business KPI normal?}
    E -->|Yes| DONE[Complete release]
    E -->|No| COMP{Schema compatible?}
    COMP -->|Yes| RB[Rollback image]
    COMP -->|No| FF[Forward fix]
    RB --> V[Verify]
    FF --> V
~~~

Rollback image an toàn khi code cũ vẫn đọc schema mới. Nếu migration đã breaking, dùng forward fix/expand-contract.

---

# Phụ Lục D: Compose Và Kubernetes

| Chủ đề | Compose baseline | Kubernetes/Helm |
|---|---|---|
| Scheduling | Docker Compose | Kubernetes Scheduler |
| Migration | migrator service | Job/Helm hook |
| Topic init | kafka-init | IaC/platform Kafka |
| Scale | giới hạn theo host | Deployment/HPA |
| Traffic | Gateway port | Ingress + Service |
| Secret | env file ngoài source | Secret manager/ExternalSecret |
| Health | Docker/Kestrel | readiness/liveness |
| Network | private network | NetworkPolicy |
| Rollout | limited rolling/recreate | RollingUpdate/PDB |
| HA state | không mặc định | phụ thuộc managed backend |

Compose là baseline một host, không tự thành HA khi chạy nhiều container. Kubernetes manifest cũng không tạo HA cho PostgreSQL/Mongo/Kafka nếu stateful backend chưa được thiết kế HA.

---

# Phụ Lục E: Có Và Chưa Có Trong Repository

## Đã có

- .NET 10 service-oriented solution.
- Aspire local orchestration.
- Gateway/YARP.
- EF migrations và PostgreSQL contexts.
- MongoDB Reporting read model.
- Kafka producer/consumer.
- Outbox/Inbox.
- Retry, backoff và DLQ.
- DLQ history, replay lease/fencing và audit.
- Idempotency cho mutation quan trọng.
- JWT role và store scope.
- Health, OpenTelemetry, Prometheus/Grafana.
- Docker, Compose, Helm và Kustomize baseline.
- Migration image/job và topic provisioning baseline.

## Chưa nên coi là production capability hoàn chỉnh

- Payment provider thật và webhook reconciliation.
- Saga Orchestrator/state machine trung tâm.
- Managed HA PostgreSQL/Mongo/Kafka.
- Multi-region failover.
- Backup/restore drill tự động.
- Secret manager cụ thể của cloud.
- Canary/blue-green controller.
- Capacity benchmark và autoscaling tuning.
- Data privacy classification theo pháp lý.

Đây là ranh giới để không overclaim khi trình bày architecture.

---

# Bài Tập Tự Học

## Sau Phase 0

- Tìm route orders trong YARP.
- Tìm project reference của Order.
- Vẽ dependency Order -> PostgreSQL/Kafka.

## Sau Phase 1

- Liệt kê invariant của Order.
- Chỉ ra dòng tạo domain event.
- Giải thích vì sao Product không sửa trực tiếp trong Order.

## Sau Phase 2

- Gửi lại cùng Idempotency-Key.
- Gửi cùng key nhưng đổi quantity.
- Dự đoán status code và database record.

## Sau Phase 3

- Giải thích publisher crash sau Kafka publish tạo duplicate thế nào.
- Chỉ ra nơi commit Kafka offset.
- Phân biệt retry transient và DLQ permanent.

## Sau Phase 4

- Giải thích vì sao Payment cần projection.
- Mô tả cách daily_sales tránh cộng hai lần.
- Nêu dữ liệu stale nào được chấp nhận.

## Sau Phase 5 và 6

- Viết release checklist cho migration thêm field.
- Chọn rollback hay forward fix sau migration.
- Thiết kế alert khi Outbox tăng nhưng request vẫn 200.


---

# Phụ Lục F: Các Service Không Dùng Kafka Trực Tiếp

## F.1. Identity Service

Identity là boundary của authentication. Nó dùng ASP.NET Core Identity và PostgreSQL; service khác không tự đọc bảng user.

### Register và Login

Flow:

1. Client gửi email/password.
2. Identity tạo hoặc xác thực user qua UserManager/SignInManager.
3. JwtTokenService đọc role và store assignment.
4. Tạo access token ngắn hạn khoảng 15 phút.
5. Tạo refresh token opaque ngẫu nhiên.
6. Chỉ hash refresh token mới được lưu vào database.
7. Trả access token và refresh token.

~~~csharp
var pair = await tokens.CreateAsync(user, db);

db.RefreshTokens.Add(new RefreshToken
{
    UserId = user.Id,
    TokenHash = JwtTokenService.Hash(pair.RefreshToken),
    ExpiresAtUtc = DateTime.UtcNow.AddDays(30)
});

await db.SaveChangesAsync();
return Results.Ok(new TokenResponse(
    pair.AccessToken,
    pair.RefreshToken));
~~~

Access token chứa sub, name, role, store_id và exp. JWT self-contained giúp service validate local, nhưng thay đổi role/store không vô hiệu hóa ngay token đã phát; token ngắn hạn giảm cửa sổ rủi ro.

### Refresh rotation

1. Client gửi refresh token.
2. Server hash token và tìm record.
3. Kiểm tra chưa revoked, chưa expired.
4. Conditional update revoke token cũ.
5. Tạo cặp token mới và lưu hash.
6. Request refresh đồng thời chỉ một request claim thành công.

RefreshTokenCleanupService xóa token hết hạn/revoked theo retention. File nguồn: src/Services/Identity/PosCafe.Identity.Api/Program.cs, src/Services/Identity/PosCafe.Identity.Infrastructure/JwtTokenService.cs và RefreshTokenCleanupService.cs.

### Role và store assignment

Khi phát JWT, Identity đưa các assignment active vào store_id claims. Service nghiệp vụ phải kiểm tra:

~~~text
principal.IsInRole(requiredRole)
AND principal.CanAccessStore(request.StoreId)
~~~

Không tin StoreId do client gửi nếu claim không cho phép.

## F.2. Catalog Service

Catalog quản lý Category và Product. Order snapshot ProductName/UnitPrice khi tạo line để lịch sử không bị thay đổi khi Catalog đổi giá.

Endpoint chính:

~~~text
GET    /api/v1/catalog/categories
POST   /api/v1/catalog/categories
GET    /api/v1/catalog/products
POST   /api/v1/catalog/products
PUT    /api/v1/catalog/products/{id}/price
DELETE /api/v1/catalog/products/{id}
~~~

Flow tạo Product:

1. Validate CategoryId, tên và giá.
2. Kiểm tra Category active.
3. Kiểm tra Idempotency-Key.
4. Tạo Product.
5. Ghi audit và idempotency record.
6. Commit PostgreSQL.

Delete là deactivate/soft delete. Các câu hỏi thiết kế giá: giá tại thời điểm Order hay giá hiện tại, ai đổi giá, có effective time/version không, và retry key cũ có trả giá cũ không?

## F.3. Store Service

Store quản lý code, name, timezone và active state:

~~~text
GET    /api/v1/stores
POST   /api/v1/stores
PUT    /api/v1/stores/{id}
DELETE /api/v1/stores/{id}
~~~

Delete là deactivate nghiệp vụ. Create/update/delete đều có idempotency và audit.

Timezone quyết định business date:

~~~text
UTC event:       2026-09-01T01:00:00Z
Store timezone:  Asia/Ho_Chi_Minh
Business date:   2026-09-01
~~~

Không dùng timezone của container để tính ngày bán.

## F.4. Kitchen Service

Kitchen API hiện là baseline mỏng, chủ yếu có ServiceDefaults, health endpoints và root endpoint. Không nên mô tả nó như kitchen workflow hoàn chỉnh.

Nếu mở rộng cần quyết định:

- Nhận OrderConfirmed hay KitchenTicketCreated?
- Có nhiều station không?
- Trạng thái là Queued, InProgress, Ready, Served, Cancelled?
- Ai phát event Ready?
- Printer/display down thì retry/DLQ ở đâu?
- Cancel sau khi bếp bắt đầu cần compensation gì?

Đây là ví dụ về architectural honesty: service tồn tại không có nghĩa business capability đã hoàn chỉnh.

---

# Phụ Lục G: Bảng Endpoint Và Quyền

| Service | Endpoint | Loại | Quyền chính |
|---|---|---|---|
| Identity | POST /identity/register | Command | public theo policy hiện tại |
| Identity | POST /identity/login | Command | public theo policy hiện tại |
| Identity | POST /identity/refresh | Command | refresh token |
| Order | POST /api/v1/orders | Command | order-operator + store |
| Order | POST /orders/{id}/confirm | Command | order-operator + store |
| Order | POST /orders/{id}/cancel | Command | order-operator + store |
| Order | GET /api/v1/orders | Query | authenticated + store |
| Payment | POST /api/v1/payments | Command | payment-operator + store |
| Payment | POST /payments/{id}/authorize | Command | payment-operator + store |
| Payment | POST /payments/{id}/refund | Command | payment-operator + store |
| Inventory | GET /api/v1/inventory | Query | authenticated + store |
| Inventory | POST /inventory/receive | Command | inventory scope |
| Inventory | PUT /inventory/adjust | Command | inventory scope |
| Inventory | POST /inventory/reserve | Command | inventory scope |
| Inventory | POST /inventory/release | Command | inventory scope |
| Catalog | GET/POST/PUT/DELETE catalog | Query/Command | catalog-manager cho mutation |
| Store | GET /api/v1/stores | Query | authenticated |
| Store | POST/PUT/DELETE stores | Command | store-manager |
| Reporting | GET /api/v1/reports/daily-sales | Query | authenticated + store |
| Reporting | POST /internal/v1/reporting/daily-sales | Internal command | internal API key |
| Gateway | POST /ops/dlq/replay | Operations | role + topic scope + rate limit |

Gateway authorization không thay thế downstream authorization. Defense in depth nghĩa là cả Gateway và service đều kiểm tra boundary cần thiết.

---

# Phụ Lục H: Configuration Được Bind Như Thế Nào?

## H.1. Configuration hierarchy

~~~text
appsettings.json
appsettings.{Environment}.json
environment variables
command-line arguments
secret/config provider
~~~

Environment variable dùng hai dấu gạch dưới cho nested key:

~~~text
ConnectionStrings__orderdb
Kafka__Security__Enabled
Security__RateLimit__PermitLimit
HealthChecks__ExposeEndpoints
~~~

Trong code:

~~~csharp
builder.Configuration.GetConnectionString("orderdb");
builder.Configuration.GetValue<bool>(
    "HealthChecks:ExposeEndpoints");
builder.Configuration.GetSection(
    "Kafka:Security");
~~~

Sai tên section là lỗi phổ biến: Kafka__BootstrapServers không được đọc bởi GetConnectionString("kafka"); key đúng là ConnectionStrings__kafka.

## H.2. Config quan trọng

| Config | Dùng ở đâu | Ý nghĩa |
|---|---|---|
| ConnectionStrings__orderdb | Order | PostgreSQL Order |
| ConnectionStrings__paymentdb | Payment | PostgreSQL Payment |
| ConnectionStrings__catalogread | Reporting/Catalog | Mongo read model |
| ConnectionStrings__kafka | Kafka services | Bootstrap servers |
| Jwt__Key | API/Gateway | JWT signing validation |
| Messaging__RequiredTopics | Kafka health | topic bắt buộc |
| Messaging__MinimumTopicPartitions | Kafka health | partition tối thiểu |
| Outbox__* | Order/Payment | publisher/consumer policy |
| Kafka__Security__* | Kafka clients | TLS/SASL |
| Observability__Prometheus__Enabled | ServiceDefaults | metrics |
| HealthChecks__ExposeEndpoints | ServiceDefaults | /health và /alive |
| Reporting__InternalApiKeys__0 | Reporting | internal write auth |

Critical config ngoài Development phải fail-fast, không silently fallback localhost.

## H.3. Config và business state

Config là deployment concern, không phải runtime business state. Không đưa Order status vào environment variable; state phải nằm trong database/event.

---

# Phụ Lục I: Lệnh Học Và Vận Hành

## I.1. Build và local

~~~text
dotnet restore
dotnet build PosCafe.slnx
dotnet build PosCafe.slnx --configuration Release
dotnet run --project src/PosCafe.AppHost/PosCafe.AppHost.csproj
~~~

Build chứng minh compile/package, không chứng minh Kafka/PostgreSQL/Mongo/Ingress healthy. Aspire dashboard dùng để xem resource, log và health.

## I.2. Docker Compose

~~~text
docker compose --env-file /run/secrets/poscafe.env   -f deploy/docker-compose.production.yml up -d --build

docker compose -f deploy/docker-compose.production.yml ps
docker compose -f deploy/docker-compose.production.yml logs -f gateway
~~~

Kiểm tra kafka-init và migrator exit code trước khi kết luận API hoạt động. Env file phải ở ngoài source control.

## I.3. Kubernetes và Helm

~~~text
kubectl apply -k deploy/k8s/
kubectl get pods -n poscafe
kubectl get jobs -n poscafe
kubectl rollout status deployment/gateway -n poscafe
kubectl describe pod <pod> -n poscafe
kubectl logs deployment/order -n poscafe

helm upgrade --install poscafe deploy/helm/poscafe   --namespace poscafe   --create-namespace   -f values.production.yaml

helm status poscafe -n poscafe
helm history poscafe -n poscafe
~~~

Secret thật phải đến từ secret manager/provider. secrets.example.yaml chỉ là mẫu.

---

# Phụ Lục J: Kafka Sizing Và Vận Hành Nâng Cao

## J.1. Partition

Partition quyết định parallelism, ordering boundary và storage/network. Ước lượng dựa trên peak messages/second, processing time, retry duration và sustainable throughput mỗi partition. Không chọn partition chỉ theo số replica API.

Ordering chỉ trong một partition. Key OrderId giúp event cùng Order đi cùng partition, nhưng không tạo ordering toàn topic.

## J.2. Replication và durability

Kafka production cần quyết định replication factor, min.insync.replicas, acks=all, retention, ACL, TLS/SASL và DLQ retention. Code producer đã dùng Acks.All, EnableIdempotence và AllowAutoCreateTopics=false; đó là client safety, không thay thế broker replication/IaC.

## J.3. Rebalance

Rebalance có thể xảy ra khi instance join/leave, partition thay đổi, consumer vượt MaxPollIntervalMs hoặc heartbeat fail. Retry quá lâu trong poll loop có thể gây rebalance. Cân bằng RetryMaxSeconds, MaxPollIntervalMs, processing time và partition count.

## J.4. Offset và replay

Offset là vị trí đọc của group, không phải trạng thái business. Commit trước side effect có thể mất message; không commit thì message giao lại. Inbox làm giao lại an toàn hơn. Replay event cụ thể thường an toàn và dễ audit hơn reset cả consumer group.

---

# Phụ Lục K: Checklist Review Feature Mới

## Domain

- [ ] Invariant nằm trong aggregate.
- [ ] Domain không phụ thuộc HTTP/Kafka/EF.
- [ ] Domain event có ý nghĩa nghiệp vụ.

## API và persistence

- [ ] Command/query phân biệt rõ.
- [ ] Role và store scope được kiểm tra.
- [ ] Exception trả contract ổn định.
- [ ] Mutation có idempotency phù hợp.
- [ ] Transaction, unique index và audit đúng boundary.
- [ ] Migration backward-compatible.

## Messaging

- [ ] Event id/schema/type bắt buộc.
- [ ] Key giữ ordering cần thiết.
- [ ] Consumer group có chủ đích.
- [ ] Outbox ghi cùng business state.
- [ ] Inbox chống duplicate.
- [ ] Offset commit sau side effect.
- [ ] Retry transient, DLQ permanent.
- [ ] DLQ giữ original topic/partition/offset.
- [ ] Replay có authorization, idempotency và audit.

## Operations

- [ ] Readiness và liveness đúng mục đích.
- [ ] Metrics không có high-cardinality label.
- [ ] Logs có event/correlation/trace id.
- [ ] Secret ngoài image/git.
- [ ] Migration/topic ordering rõ.
- [ ] Rollback sau migration đã phân tích.
- [ ] Alert và runbook đã có.

---

# Lộ Trình Học 10 Ngày

| Ngày | Chủ đề | Kết quả |
|---|---|---|
| 1 | Solution, Aspire, Gateway | Lần được request tới API |
| 2 | DDD Order | Viết được invariant/state transition |
| 3 | CQRS/API/auth | Giải thích command/query và 401/403/409 |
| 4 | PostgreSQL/EF | Chỉ ra transaction và migration |
| 5 | Kafka fundamentals | Giải thích topic/partition/key/group/offset |
| 6 | Outbox | Vẽ dual-write problem và publisher |
| 7 | Inbox/retry/DLQ | Giải thích crash window và commit offset |
| 8 | Saga/consistency | Vẽ success/failure/compensation |
| 9 | Mongo/observability | Tìm projection lag bằng log/metric |
| 10 | Deploy/recovery/review | Viết release và rollback plan |

Sau mỗi ngày, trả lời bằng code/path cụ thể. “Outbox là gì?” chưa đủ; hãy chỉ ra bảng, class, transaction, publisher, metric và failure khi Kafka down.


---

# Phụ Lục L: Đọc Code Theo Từng Dòng

Phần này là cách học hiệu quả nhất: không học thuộc định nghĩa, mà đọc một đoạn code rồi tự trả lời “nó bảo vệ điều gì, failure nào xảy ra, dữ liệu nằm ở đâu?”.

## L.1. AppHost: dependency graph local

Đoạn trong src/PosCafe.AppHost/AppHost.cs:

~~~csharp
var postgres = builder.AddPostgres("postgres");
var kafka = builder.AddKafka("kafka");

var orderDb = postgres.AddDatabase("orderdb");

var order = builder.AddProject<Projects.PosCafe_Order_Api>("order")
    .WithReference(orderDb)
    .WithReference(kafka)
    .WaitFor(postgres)
    .WaitFor(kafka);

builder.AddProject<Projects.PosCafe_ApiGateway>("gateway")
    .WithReference(order)
    .WaitFor(order);
~~~

Đọc từng câu:

1. AddPostgres tạo resource logical tên postgres.
2. AddDatabase không nhất thiết tạo một PostgreSQL server mới; nó tạo database/reference orderdb trong resource.
3. AddProject đăng ký process Order API.
4. WithReference cho phép Aspire đưa connection information vào configuration.
5. WaitFor(postgres) chỉ nói process nên chờ resource PostgreSQL ready trước khi khởi động.
6. WaitFor(kafka) làm cùng việc cho Kafka.
7. Gateway WithReference(order) dùng service discovery local, không hard-code port.
8. Gateway WaitFor(order) giảm lỗi startup race, nhưng không thay thế readiness check.

Bài học kiến trúc: startup ordering và runtime resilience là hai chuyện khác nhau. Một process có thể start sau Kafka nhưng Kafka có thể chết 10 phút sau; publisher/consumer vẫn phải retry và health phải phản ánh trạng thái đó.

## L.2. Request middleware: correlation ID đi qua HTTP

File src/PosCafe.ServiceDefaults/PosCafeRequestMiddleware.cs:

~~~csharp
var correlationId =
    context.Request.Headers["X-Correlation-Id"].ToString();

if (string.IsNullOrWhiteSpace(correlationId)
    || correlationId.Length > 128
    || correlationId.Any(char.IsControl))
{
    correlationId =
        Activity.Current?.TraceId.ToString()
        ?? Guid.NewGuid().ToString("N");
}

context.TraceIdentifier = correlationId;
context.Request.Headers["X-Correlation-Id"] = correlationId;
~~~

Từng bước:

1. Ưu tiên ID caller gửi để giữ liên kết từ frontend/API client.
2. Từ chối chuỗi rỗng, quá dài hoặc có control character.
3. Nếu caller không gửi ID, dùng Activity trace id.
4. Nếu chưa có Activity, tạo GUID.
5. Ghi vào TraceIdentifier để logging framework có thể dùng.
6. Ghi lại request header để downstream code đọc được cùng giá trị.
7. Response trả X-Correlation-Id để client gửi cho support.

Điểm cần cải thiện khi scale lớn: correlation nghiệp vụ và trace id không nhất thiết phải là một giá trị. Một design chặt chẽ có thể giữ X-Correlation-Id cho business flow, còn W3C traceparent cho trace span. Repository hiện dùng Activity.TraceId làm fallback/giá trị correlation trong nhiều command service; tài liệu phải ghi đúng implementation này.

## L.3. OrderCommandService: transaction boundary

File src/Services/Order/PosCafe.Order.Infrastructure/OrderCommandService.cs:

~~~csharp
await using var transaction =
    await db.Database.BeginTransactionAsync(token);

var order = await action();

var correlationId =
    Activity.Current?.TraceId.ToString()
    ?? Guid.NewGuid().ToString("N");

db.AuditEntries.Add(new AuditEntry
{
    Action = auditAction,
    EntityType = "Order",
    EntityId = order.Id.ToString(),
    ActorId = actorId,
    StoreId = order.StoreId,
    CorrelationId = correlationId,
    OccurredAtUtc = DateTime.UtcNow
});

foreach (var domainEvent in order.DequeueDomainEvents())
{
    db.OutboxMessages.Add(ToOutbox(
        order, domainEvent, correlationId));
}

await db.SaveChangesAsync(token);
await transaction.CommitAsync(token);
~~~

Tại sao transaction bắt đầu trước action?

- Action load hoặc tạo aggregate.
- Domain thay đổi state và raise events.
- Audit và Outbox được thêm vào cùng DbContext.
- SaveChanges ghi tất cả change tracking.
- Commit là điểm xác nhận business operation bền vững.

Tại sao không publish Kafka ở đây?

- Kafka network call có thể chậm.
- Giữ PostgreSQL transaction trong lúc chờ broker làm lock lâu.
- Nếu Kafka publish thành công nhưng database commit fail, vẫn có dual-write inconsistency.
- Outbox chuyển network call sang background publisher.

Tại sao correlation được ghi trong audit và Outbox?

- Audit tìm được toàn flow.
- Publisher giữ lại correlation khi tạo header.
- Consumer log có thể nối ngược về request.

## L.4. Outbox publisher: claim trước, publish sau

OrderOutboxPublisher chia thành hai phase:

### Phase 1: claim

~~~sql
SELECT ...
FROM outbox_messages
WHERE "ProcessedOnUtc" IS NULL
  AND "DeadLetteredOnUtc" IS NULL
  AND "Attempts" < @maxAttempts
  AND ("LockedUntilUtc" IS NULL OR "LockedUntilUtc" < @now)
ORDER BY "OccurredOnUtc"
LIMIT @batchSize
FOR UPDATE SKIP LOCKED
~~~

Mục đích của từng điều kiện:

- ProcessedOnUtc null: chưa publish thành công.
- DeadLetteredOnUtc null: chưa bị kết thúc thất bại.
- Attempts < max: chưa vượt retry policy.
- LockedUntilUtc null/hết hạn: chưa bị instance khác giữ.
- ORDER BY OccurredOnUtc: ưu tiên event cũ hơn.
- LIMIT: giới hạn memory và transaction size.
- FOR UPDATE: khóa row trong claim transaction.
- SKIP LOCKED: cho phép replica khác lấy row khác.

Sau khi lấy, publisher set lease:

~~~csharp
message.LockedUntilUtc =
    now.AddSeconds(options.Value.LeaseSeconds);
message.Attempts++;
~~~

Sau đó commit claim transaction trước khi gọi Kafka. Nếu process chết lúc đang publish, lease hết hạn để worker khác thử lại.

### Phase 2: publish

~~~csharp
await producer.ProduceAsync(
    options.Value.Topic,
    new Message<string, string>
    {
        Key = message.AggregateId,
        Value = message.Payload,
        Headers = headers
    },
    cancellationToken);

message.ProcessedOnUtc = DateTime.UtcNow;
message.LockedUntilUtc = null;
message.Error = null;
~~~

Chỉ mark ProcessedOnUtc sau await ProduceAsync thành công. Tuy nhiên crash window vẫn tồn tại:

~~~text
Kafka đã nhận message
process chết
ProcessedOnUtc chưa được ghi
worker mới publish lại
~~~

Đây là lý do consumer phải coi duplicate là bình thường, không phải exception bất ngờ.

## L.5. InboxProcessor: duplicate gate

BuildingBlocks/BuildingBlocks/Messaging/InboxProcessor.cs có khóa logic:

~~~csharp
var existing = await db.Set<InboxMessage>()
    .SingleOrDefaultAsync(
        x => x.EventId == eventId
          && x.Consumer == consumer,
        cancellationToken);

if (existing is not null)
    return existing.ProcessedOnUtc is null;
~~~

Nếu record tồn tại:

- ProcessedOnUtc khác null: đã xử lý xong, trả false.
- ProcessedOnUtc null: trước đó chưa hoàn tất, cho phép retry.

Record có key:

~~~text
EventId + Consumer
~~~

Không dùng chỉ EventId vì cùng một event phải được Payment, Inventory và Reporting xử lý độc lập.

Race condition:

1. Consumer A và B cùng nhận duplicate.
2. Cả hai cùng không thấy Inbox row.
3. Một consumer insert thành công.
4. Consumer còn lại gặp unique constraint DbUpdateException.
5. Consumer còn lại clear tracker và coi đó là duplicate/race.

Database unique key là lớp bảo vệ cuối cùng. Kiểm tra trước bằng SELECT chỉ là tối ưu, không phải guarantee.

## L.6. Payment OrderEventHandler: side effect và offset

Payment handler:

~~~csharp
if (!await InboxProcessor.TryStartAsync(
        db, eventId, consumer, cancellationToken))
{
    MessagingMetrics.DuplicateEvents.Add(
        1,
        new KeyValuePair<string, object?>(
            "service", "payment"));
    return false;
}

if (eventType == "OrderConfirmed.v1")
{
    IntegrationPayloadValidator.Validate(eventType, payload);
    var confirmed =
        JsonSerializer.Deserialize<OrderConfirmedEvent>(payload)
        ?? throw new InvalidOperationException(
            "OrderConfirmed payload is invalid.");

    var projection = await db.OrderProjections
        .SingleOrDefaultAsync(
            x => x.OrderId == confirmed.OrderId,
            cancellationToken);

    if (projection is null)
        db.OrderProjections.Add(...);
    else
        update projection;
}

await InboxProcessor.MarkProcessedAsync(
    db, eventId, consumer, cancellationToken);
~~~

Điểm subtle:

- TryStart và MarkProcessed nằm cùng DbContext.
- Consumer bên ngoài mở transaction trước khi gọi handler.
- Projection và Inbox mark commit cùng nhau.
- Consumer.Commit(result) chỉ gọi sau transaction commit.
- Nếu deserialize hoặc database fail, transaction rollback và offset không commit.
- Kafka giao lại event, attempt tăng, handler thử lại.

Nếu handler gọi một payment provider ngoài transaction:

~~~text
DB update -> provider call -> mark inbox
~~~

thì cần provider idempotency key. Database Inbox không rollback được external charge. Đây là boundary quan trọng khi hệ thống thêm payment gateway thật.

## L.7. MongoReportingRepository: transaction read model

Reporting không dùng EF Inbox mà có processed_reporting_events. Code:

~~~csharp
session.StartTransaction();

await events.InsertOneAsync(
    session,
    new ProcessedReportingEvent(eventId, DateTime.UtcNow),
    cancellationToken: cancellationToken);

var update = Builders<DailySalesReadModel>.Update
    .Inc(x => x.GrossSales, total)
    .Inc(x => x.OrderCount, 1)
    .Set(x => x.UpdatedAtUtc, DateTime.UtcNow)
    .SetOnInsert(x => x.StoreId, storeId)
    .SetOnInsert(x => x.BusinessDate, businessDate);

await collection.UpdateOneAsync(
    session,
    filter,
    update,
    new UpdateOptions { IsUpsert = true },
    cancellationToken);

await session.CommitTransactionAsync(cancellationToken);
~~~

Insert processed event và increment sales cùng transaction để không có trạng thái “đã đánh dấu processed nhưng chưa cộng tiền” hoặc ngược lại.

Unique index:

~~~text
processed_reporting_events.EventId unique
daily_sales.StoreId + BusinessDate unique
~~~

Duplicate event gặp DuplicateKey, transaction abort và không increment lần hai.

---

# Phụ Lục M: Full Flow Với Timeline Và Trạng Thái

## M.1. Timeline thành công

| Thời điểm | Thành phần | Hành động | Trạng thái bền vững |
|---|---|---|---|
| t0 | Client | Gửi Create Order | Chưa có |
| t1 | Gateway | Validate/forward | Chưa có |
| t2 | Order API | Validate role/store/key | Chưa có |
| t3 | Order Domain | Tạo aggregate | Memory |
| t4 | Order DB | Commit order/audit/idempotency | Order tồn tại |
| t5 | Order DB | Commit outbox | Event chưa publish |
| t6 | Publisher | Claim lease | Attempts tăng |
| t7 | Kafka | Append topic | Event tồn tại |
| t8 | Publisher | Mark processed | Outbox hoàn tất |
| t9 | Payment | Consume + projection | Payment projection tồn tại |
| t10 | Inventory | Consume + reserve | Stock giảm/reserved |
| t11 | Reporting | Mongo transaction | daily_sales tăng |
| t12 | Consumer | Commit offsets | Message đã được xác nhận |

t4 và t5 thực tế nằm trong cùng transaction/commit. t9, t10, t11 không đồng bộ với response ở t4.

## M.2. Timeline Kafka fail

~~~mermaid
sequenceDiagram
    participant API as Order API
    participant DB as PostgreSQL
    participant P as Publisher
    participant K as Kafka

    API->>DB: Commit Order + Outbox
    P->>DB: Claim + lease
    P-xK: Produce timeout
    P->>DB: Save Error + backoff
    Note over DB: Order vẫn tồn tại
    P->>K: Retry sau lease
    K-->>P: Success
    P->>DB: ProcessedOnUtc
~~~

Client không nên nhận “Order mất” chỉ vì Kafka đang outage. Nếu API transaction thành công, Order là source of truth; event sẽ được phát lại từ Outbox.

## M.3. Timeline consumer fail sau side effect

~~~mermaid
sequenceDiagram
    participant K as Kafka
    participant C as Payment
    participant DB as Payment DB

    K->>C: Event E
    C->>DB: Projection + Inbox processed
    DB-->>C: Commit thành công
    C-xK: Process crash trước offset commit
    K->>C: Event E giao lại
    C->>DB: Inbox thấy E đã processed
    C->>K: Commit offset, không side effect lần hai
~~~

Nếu crash trước DB commit, transaction rollback và lần sau xử lý lại bình thường.

## M.4. Bảng trạng thái Outbox

| Trạng thái | ProcessedOnUtc | DeadLetteredOnUtc | Lease | Ý nghĩa |
|---|---|---|---|---|
| Pending | null | null | null | Chưa claim |
| Claimed | null | null | tương lai | Một worker đang xử lý |
| Retryable | null | null | quá hạn/tương lai | Đợi retry |
| Published | có giá trị | null | null | Đã publish thành công |
| Dead-lettered | null | có giá trị | null | Đã kết thúc thất bại |
| Ambiguous | null | null | có thể có | Có thể Kafka đã nhận nhưng DB chưa mark; consumer phải idempotent |

---

# Phụ Lục N: Công Cụ Dùng Trong Dự Án

## N.1. .NET CLI

~~~text
dotnet restore
~~~

Tải dependency theo project/assets. Restore thành công không có nghĩa code compile.

~~~text
dotnet build PosCafe.slnx --configuration Release
~~~

Compile toàn solution và phát hiện reference/type/config compile issue. Đây là gate cần có trước khi đóng gói image.

~~~text
dotnet run --project src/PosCafe.AppHost/PosCafe.AppHost.csproj
~~~

Chạy AppHost và toàn bộ local dependency graph.

## N.2. EF Core CLI

~~~text
dotnet ef migrations list   --project src/Services/Order/PosCafe.Order.Infrastructure/PosCafe.Order.Infrastructure.csproj   --startup-project src/Services/Order/PosCafe.Order.Api/PosCafe.Order.Api.csproj
~~~

Liệt kê migration đã tồn tại trong code/design-time context.

~~~text
dotnet ef database update   --project src/Services/Order/PosCafe.Order.Infrastructure/PosCafe.Order.Infrastructure.csproj   --startup-project src/Services/Order/PosCafe.Order.Api/PosCafe.Order.Api.csproj
~~~

Apply schema. Production nên chạy qua migration release job, không chạy tùy tiện từ laptop.

## N.3. Docker và Compose

Dockerfile có build stage và runtime stage. Build stage cần SDK; runtime stage chỉ cần ASP.NET runtime, giúp image nhỏ hơn.

Compose quản lý nhiều container và dependency condition:

~~~text
docker compose --env-file /run/secrets/poscafe.env   -f deploy/docker-compose.production.yml up -d --build

docker compose -f deploy/docker-compose.production.yml ps
docker compose -f deploy/docker-compose.production.yml logs migrator
docker compose -f deploy/docker-compose.production.yml logs kafka-init
~~~

Phân biệt:

- up -d: tạo/chạy background.
- --build: build image local.
- ps: trạng thái container, không đủ để biết business healthy.
- logs: evidence khi startup/migration/consumer fail.

## N.4. Kafka CLI

Với Redpanda local, rpk có thể dùng:

~~~text
rpk topic list --brokers localhost:9092
rpk topic describe pos.order.events --brokers localhost:9092
rpk topic consume pos.order.events --brokers localhost:9092
rpk group list --brokers localhost:9092
rpk group describe pos-payment-order-events-v1 --brokers localhost:9092
~~~

Trong production, chỉ operator được cấp ACL đọc/inspect. Không reset group hoặc xóa topic để “fix” mà chưa hiểu retention/replay impact.

## N.5. Kubernetes CLI

~~~text
kubectl apply -k deploy/k8s/
kubectl get deploy,svc,pods,jobs -n poscafe
kubectl rollout status deployment/order -n poscafe
kubectl logs deployment/order -n poscafe
kubectl describe pod <pod> -n poscafe
kubectl get events -n poscafe --sort-by=.lastTimestamp
~~~

Đọc theo thứ tự:

1. Job migration.
2. Pod status.
3. Pod logs.
4. Readiness events.
5. Service endpoints.
6. Ingress/network policy.

Pod Running không đồng nghĩa Service có endpoint Ready.

## N.6. Helm và Kustomize

Helm là template/package manager. Dùng values để thay image, replicas, secret name, ingress và resource.

Kustomize là overlay/resource composition, phù hợp manifest YAML có sẵn. deploy/k8s/kustomization.yaml gom namespace, configmap, migration job, Gateway, APIs và network policy.

Không nhầm:

- Helm hook là lifecycle của Helm release.
- Kubernetes Job là workload.
- Kustomize order trong resources không đảm bảo runtime dependency.
- Readiness/Job status mới là evidence.

## N.7. Prometheus và Grafana

Prometheus pull metrics từ target. Grafana query Prometheus và hiển thị dashboard/alert.

Cách tư duy:

~~~text
Application emits metric
  -> Prometheus scrapes
  -> rule evaluates
  -> alert/dashboard shows symptom
  -> operator follows runbook
~~~

Metric cho biết “có vấn đề”; log/trace/event id giúp biết “vì sao”.

## N.8. curl và HTTP inspection

~~~text
curl -i http://localhost:8080/health
curl -i http://localhost:8080/alive
curl -i -X POST http://localhost:8080/api/v1/orders   -H "Authorization: Bearer <token>"   -H "Idempotency-Key: request-001"   -H "X-Correlation-Id: flow-001"   -H "Content-Type: application/json"   --data @order.json
~~~

Dùng -i để xem status và response headers, đặc biệt:

- X-Correlation-Id.
- Idempotency-Replayed.
- Content-Type.
- Security headers.

---

# Phụ Lục O: Thiết Kế Failure Cho Từng Thành Phần

## O.1. PostgreSQL

### Failure

- Connection refused.
- Lock timeout.
- Unique constraint conflict.
- Serialization/concurrency conflict.
- Migration permission denied.
- Disk full.

### Phản ứng

- Request transaction rollback.
- Background worker retry có giới hạn.
- Không xóa record để làm health xanh.
- Alert DB connection/lock/error.
- Migration fail chặn rollout.

### Câu hỏi

- Transaction có retry được mà không tạo duplicate không?
- Unique conflict là race bình thường hay data corruption?
- Readiness có nên fail hay process vẫn sống?

## O.2. Kafka

### Failure

- Broker unavailable.
- Authentication/ACL denied.
- Topic thiếu.
- Partition không đủ.
- Consumer rebalance.
- Produce timeout.
- Poison payload.

### Phản ứng

- Outbox giữ event.
- Kafka producer retry.
- Readiness topic check fail.
- Consumer không commit offset khi side effect fail.
- Poison event vào DLQ.
- Operator replay sau sửa nguyên nhân.

## O.3. MongoDB

### Failure

- Replica set chưa initialized.
- Primary unavailable.
- Transaction unsupported.
- Duplicate key.
- Index creation fail.
- Projection write timeout.

### Phản ứng

- Reporting readiness fail.
- Consumer retry.
- Unique processed event bảo vệ duplicate.
- Không trả báo cáo “đúng” khi projection chưa cập nhật.
- Alert projection lag.

## O.4. Gateway

### Failure

- Downstream API unhealthy.
- YARP destination sai DNS.
- JWT invalid.
- Ops DB unavailable.
- DLQ replay Kafka unavailable.
- Rate limit exceeded.

### Phản ứng

- 502 cho routing failure.
- 401/403 cho auth.
- Ops endpoint không cho replay nếu lease/audit không an toàn.
- Không public health/metrics tùy tiện.
- Theo dõi downstream latency/error.

---

# Phụ Lục P: Contract Và Schema Evolution

## P.1. Vì sao JSON schema chưa đủ?

Schema validation chỉ trả lời payload có đúng shape hay không. Nó không trả lời:

- Field có còn cùng business meaning không?
- Consumer cũ có hiểu event mới không?
- Event đã replay sau 90 ngày có còn xử lý được không?
- Database projection có cần migration không?
- Producer có gửi header schema-id đúng không?

## P.2. Quy tắc backward compatibility

An toàn hơn:

- Thêm field optional.
- Giữ field cũ trong một thời gian.
- Không đổi meaning/units.
- Tăng schema id/version có chủ ý.
- Consumer bỏ qua field không biết nếu contract cho phép.
- Deploy consumer hiểu version mới trước producer phát version mới.

Rủi ro:

- Đổi decimal thành string.
- Đổi timezone/đơn vị tiền.
- Đổi OrderId semantics.
- Rename field và xóa ngay.
- Reuse schema id cho payload meaning khác.

## P.3. Header và payload phải khớp

Consumer hiện kiểm tra cả:

~~~text
event-type
event-id
schema-version
schema-id
payload shape
~~~

Nếu header nói OrderConfirmed.v1 nhưng payload lại PaymentCreated.v1, đó là poison message dù JSON hợp lệ. DLQ reason phải cho biết mismatch nào.

---

# Phụ Lục Q: Bài Tập Có Đáp Án Kỳ Vọng

## Q.1. Kafka publish fail sau Confirm

**Câu hỏi:** Order có tồn tại không? Payment có biết không?

**Đáp án kỳ vọng:**

- Nếu transaction PostgreSQL commit thành công, Order và Outbox tồn tại.
- Payment chưa biết cho tới khi publisher publish lại.
- Không tạo Order thứ hai khi client retry cùng idempotency key.
- Theo dõi Outbox Error/Attempts và Kafka health.
- Không xóa Outbox.

## Q.2. Duplicate OrderConfirmed

**Câu hỏi:** Reporting có cộng doanh thu hai lần không?

**Đáp án kỳ vọng:**

- Không, nếu EventId đã có trong processed_reporting_events.
- Unique EventId và Mongo transaction bảo vệ.
- Consumer có thể commit offset của duplicate sau khi skip.
- Metric duplicate/consumer log giúp xác nhận.

## Q.3. Payment projection chưa có

**Câu hỏi:** Có query chéo Order database để tạo Payment ngay không?

**Đáp án kỳ vọng:**

- Không nếu boundary đã chọn projection.
- Trả trạng thái chưa đồng bộ hoặc retry.
- Kiểm tra event đã publish/consume chưa.
- Nếu SLA yêu cầu mạnh hơn, thiết kế synchronous validation/API contract mới, không hack query chéo.

## Q.4. Migration đã chạy, code mới fail

**Câu hỏi:** Có rollback database ngay không?

**Đáp án kỳ vọng:**

- Không mặc định.
- Kiểm tra migration có backward-compatible không.
- Nếu có, rollback image code.
- Nếu breaking, forward fix hoặc restore theo disaster plan.
- Không chạy migration ngược tùy tiện trên production.

## Q.5. Correlation ID nào dùng để debug?

**Câu hỏi:** Có dùng OrderId, EventId hay TraceId không?

**Đáp án kỳ vọng:**

- OrderId: tìm business entity.
- EventId: tìm một message cụ thể.
- CorrelationId: nối business flow.
- TraceId: nối distributed spans.
- Topic/partition/offset: vị trí Kafka.
- Không dùng một ID thay thế toàn bộ semantics.

## Q.6. Retry vô hạn có tốt không?

**Câu hỏi:** Vì sao không retry cho đến khi thành công?

**Đáp án kỳ vọng:**

- Lỗi schema/data vĩnh viễn sẽ chặn partition.
- Backlog tăng vô hạn.
- Consumer group có thể không tiến.
- DLQ cô lập poison message.
- Retry policy phải có max attempts, backoff, alert và replay process.

---

# Phụ Lục R: Tiêu Chuẩn “Đã Hiểu” Một Component

Bạn chỉ nên nói đã hiểu Outbox khi có thể trả lời không nhìn tài liệu:

1. Row Outbox được tạo ở đâu?
2. Nó có cùng transaction với business state không?
3. Worker claim bằng cơ chế gì?
4. Lease hết hạn xử lý ra sao?
5. Publish thành công mark cột nào?
6. Crash sau publish trước mark tạo điều gì?
7. Consumer chống duplicate bằng gì?
8. Max attempts ở đâu?
9. DLQ giữ metadata nào?
10. Metric/log nào chứng minh backlog?

Bạn chỉ nên nói đã hiểu deployment khi có thể trả lời:

1. Image nào được build?
2. Config/secret inject ở đâu?
3. Topic được tạo bởi ai?
4. Migration chạy trước API thế nào?
5. Readiness khác liveness ra sao?
6. Service name có khớp YARP không?
7. Pod Security yêu cầu gì?
8. Rollback sau migration có an toàn không?
9. Alert nào báo lỗi?
10. Runbook đầu tiên cần mở là file nào?


