package com.dobrev.auditservice.products.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@DynamoDbBean
@Getter
@Setter
@Builder
public class ProductInfoEvent {
    private String id;
    private String code;
    private Float price;
    private String messageId;
    private String requestId;
    private String traceId;
}
