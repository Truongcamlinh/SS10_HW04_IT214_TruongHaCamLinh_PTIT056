# SS10 HW04: Xử lý lỗi Kafka Consumer với Retry và Dead Letter Queue

**Sinh viên:** Truong Ha Cam Linh  
**Lớp:** IT214  
**Mã:** PTIT056

## 1. Bối cảnh

`InventoryService` lắng nghe topic `order-events`. Mỗi khi có đơn hàng mới, consumer nhận `OrderEvent` và gọi nghiệp vụ trừ kho theo `productId`, `quantity`.

Vấn đề xảy ra khi topic có message lỗi, ví dụ JSON sai định dạng hoặc dữ liệu nghiệp vụ không hợp lệ. Nếu consumer chỉ throw exception mà không có cơ chế xử lý lỗi, nó có thể bị kẹt tại offset lỗi và không xử lý được các đơn hàng hợp lệ phía sau.

## 2. Cơ chế offset của Kafka

Trong Kafka, mỗi message trong một partition có một offset tăng dần. Consumer đọc message theo thứ tự offset trong partition. Khi consumer xử lý xong một message, offset mới được commit để Kafka biết rằng group đó đã xử lý tới vị trí nào.

Nếu message ở offset `10` xử lý thành công, consumer có thể commit offset tiếp theo. Nhưng nếu offset `11` gây exception và offset chưa được commit, lần poll sau consumer group vẫn đọc lại offset `11`. Vì vậy consumer cứ lặp lại đúng message lỗi.

Đây là lý do hệ thống bị kẹt: Kafka đang bảo vệ tính không mất dữ liệu. Nó không tự bỏ qua message lỗi nếu ứng dụng chưa nói rõ phải xử lý message đó như thế nào.

## 3. Vì sao consumer bị kẹt khi gặp JSON sai

Với đoạn code cũ:

```java
@KafkaListener(topics = "order-events", groupId = "inventory-group")
public void consume(OrderEvent event) {
    inventoryService.deductStock(event.getProductId(), event.getQuantity());
}
```

Nếu message không deserialize được thành `OrderEvent`, hoặc `deductStock()` ném exception, listener thất bại. Không có retry có kiểm soát, không có DLQ, nên message lỗi cứ được đọc lại. Các message phía sau trong cùng partition không được xử lý tiếp vì Kafka cần giữ đúng thứ tự offset.

## 4. Thiết kế xử lý lỗi

Bài này dùng:

- `ErrorHandlingDeserializer` để bắt lỗi JSON/deserialization.
- `DefaultErrorHandler` để retry có kiểm soát.
- `FixedBackOff(1000L, 3L)` để retry tối đa 3 lần, mỗi lần cách nhau 1 giây.
- `DeadLetterPublishingRecoverer` để gửi message lỗi sang topic `order-events.DLQ`.
- Sau khi message lỗi được đưa vào DLQ, consumer có thể commit/bỏ qua offset lỗi và tiếp tục đọc message mới.

## 5. File chính

```text
src/main/java/com/storex/inventory/consumer/InventoryConsumer.java
src/main/java/com/storex/inventory/config/KafkaErrorHandlerConfig.java
src/main/resources/application.yml
```

## 6. Cấu hình quan trọng

```yaml
spring:
  kafka:
    consumer:
      group-id: inventory-group
      enable-auto-commit: false
      value-deserializer: org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
      properties:
        spring.deserializer.value.delegate.class: org.springframework.kafka.support.serializer.JsonDeserializer
        spring.json.trusted.packages: com.storex.inventory.model
        spring.json.value.default.type: com.storex.inventory.model.OrderEvent
    listener:
      ack-mode: record
```

## 7. ErrorHandler

```java
DefaultErrorHandler errorHandler = new DefaultErrorHandler(
        recoverer,
        new FixedBackOff(1000L, 3L));
```

Khi xử lý thất bại:

1. Consumer thử xử lý message ban đầu.
2. Nếu lỗi, retry lần 1.
3. Nếu vẫn lỗi, retry lần 2.
4. Nếu vẫn lỗi, retry lần 3.
5. Nếu vẫn thất bại, message được gửi sang `order-events.DLQ`.
6. Consumer tiếp tục xử lý message tiếp theo, không bị kẹt.

## 8. Kết quả mong đợi

- Message hợp lệ được xử lý bình thường.
- Message lỗi được retry tối đa 3 lần.
- Message vẫn lỗi sau retry được đưa vào DLQ.
- Consumer không bị đứng ở một offset lỗi.
- Log có thông tin topic, partition, offset và nguyên nhân lỗi để debug.

## 9. Chạy kiểm tra

```bash
./gradlew build
```

Khi chạy thật cần Kafka ở `localhost:9092` và tạo topic:

```text
order-events
order-events.DLQ
```
