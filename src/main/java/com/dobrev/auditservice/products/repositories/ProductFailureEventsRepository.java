package com.dobrev.auditservice.products.repositories;

import com.dobrev.auditservice.events.dto.ProductEventType;
import com.dobrev.auditservice.events.dto.ProductFailureEventDto;
import com.dobrev.auditservice.products.models.ProductFailureEvent;
import com.dobrev.auditservice.products.models.ProductInfoFailureEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Repository
public class ProductFailureEventsRepository {
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<ProductFailureEvent> productFailureEventsTable;

    public ProductFailureEventsRepository(@Value("${aws.events.ddb}") String eventsDdbName,
                                          DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.productFailureEventsTable = dynamoDbEnhancedAsyncClient.table(eventsDdbName,
                TableSchema.fromBean(ProductFailureEvent.class));
    }

    public CompletableFuture<Void> create(ProductFailureEventDto productFailureEventDto,
                                          ProductEventType productEventType,
                                          String messageId, String requestId, String traceId) {
        long timestamp = Instant.now().toEpochMilli();
        long ttl = Instant.now().plusSeconds(300).getEpochSecond();

        var productInfoFailureEvent = ProductInfoFailureEvent.builder()
                .id(productFailureEventDto.id())
                .messageId(messageId)
                .requestId(requestId)
                .traceId(traceId)
                .error(productFailureEventDto.error())
                .status(productFailureEventDto.status())
                .build();

        var productFailureEvent = ProductFailureEvent.builder()
                .pk("#product_".concat(productEventType.name()))
                .sk(String.valueOf(timestamp))
                .createdAt(timestamp)
                .ttl(ttl)
                .email(productFailureEventDto.email())
                .info(productInfoFailureEvent)
                .build();

        return productFailureEventsTable.putItem(productFailureEvent);
    }
}