package com.dobrev.auditservice.products.repositories;

import com.amazonaws.xray.spring.aop.XRayEnabled;
import com.dobrev.auditservice.events.dto.ProductEventDto;
import com.dobrev.auditservice.events.dto.ProductEventType;
import com.dobrev.auditservice.products.models.ProductEvent;
import com.dobrev.auditservice.products.models.ProductInfoEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Repository
@Slf4j
@XRayEnabled
public class ProductEventsRepository {
    private final DynamoDbEnhancedAsyncClient dynamoDbEnhancedAsyncClient;
    private final DynamoDbAsyncTable<ProductEvent> eventsTable;

    public ProductEventsRepository(@Value("${aws.events.ddb}") String eventsDdbName,
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
                .pk(buildProductPartitionKey(productEventType.name()))
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

    public SdkPublisher<Page<ProductEvent>> findByType(String productEventType, String exclusiveStartTimestamp, int limit){
        String pk = buildProductPartitionKey(productEventType);
        return eventsTable.query(QueryEnhancedRequest.builder()
                        .queryConditional(QueryConditional.keyEqualTo(Key.builder().partitionValue(pk).build()))
                        .exclusiveStartKey(buildExclusiveStartKey(pk, exclusiveStartTimestamp))
                        .limit(limit)
                .build())
                .limit(1);
    }

    public SdkPublisher<Page<ProductEvent>> findByTypeAndRange(String productEventType, String exclusiveStartTimestamp,
                                                               String from, String to, int limit){
        String pk = buildProductPartitionKey(productEventType);
        return eventsTable.query(QueryEnhancedRequest.builder()
                        .queryConditional(QueryConditional.sortBetween(
                                Key.builder().partitionValue(pk).sortValue(from).build(),
                                Key.builder().partitionValue(pk).sortValue(to).build()))
                        .exclusiveStartKey(buildExclusiveStartKey(pk, exclusiveStartTimestamp))
                        .limit(limit)
                .build())
                .limit(1);
    }

    private String buildProductPartitionKey(String productEventType) {
        return "#product_".concat(productEventType);
    }

    private Map<String, AttributeValue> buildExclusiveStartKey(String pk, String exclusiveStartTimestamp){
        return (exclusiveStartTimestamp != null) ?
                Map.of(
                        "pk", AttributeValue.builder().s(pk).build(),
                        "sk", AttributeValue.builder().s(exclusiveStartTimestamp).build())
                : null;
    }
}