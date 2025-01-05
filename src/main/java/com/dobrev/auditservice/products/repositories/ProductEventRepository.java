package com.dobrev.auditservice.products.repositories;

import com.dobrev.auditservice.events.dto.ProductEventDto;
import com.dobrev.auditservice.events.dto.ProductEventType;
import com.dobrev.auditservice.products.models.ProductEvent;
import com.dobrev.auditservice.products.models.ProductInfoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Repository
@Slf4j
public class ProductEventRepository {
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<ProductEvent> eventsTable;

    public ProductEventRepository(@Value("${aws.events.ddb}") String eventsDdbName,
                                  DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient,
                                  DynamoDbAsyncTable<ProductEvent> eventsTable) {
        this.dynamoDbEnhancedAsyncClient = dynamoDbEnhancedAsyncClient;
        this.eventsTable = dynamoDbEnhancedAsyncClient.table(eventsDdbName, TableSchema.fromBean(ProductEvent.class));
    }

    public CompletableFuture<Void> create(ProductEventDto productEventDto,
                                          ProductEventType productEventType,
                                          String messageId, String requestId, String traceId){
        long timestamp = Instant.now().toEpochMilli();
        long ttl = Instant.now().plusSeconds(300).getEpochSecond();

        var productEvent = ProductEvent.builder()
                .pk("#product_".concat(productEventType.name()))
                .sk(String.valueOf(timestamp))
                .createdAt(timestamp)
                .ttl(ttl)
                .email(productEventDto.email())
                .build();

        var productInfoEvent = ProductInfoEvent.builder()
                .code(productEventDto.code())
                .id(productEventDto.id())
                .price(productEventDto.price())
                .messageId(messageId)
                .requestId(requestId)
                .traceId(traceId)
                .build();

        return eventsTable.putItem(productEvent);
    }
}