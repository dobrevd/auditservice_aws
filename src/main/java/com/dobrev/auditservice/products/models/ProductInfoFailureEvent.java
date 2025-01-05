package com.dobrev.auditservice.products.models;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@DynamoDbBean
@Getter
@Setter
@Builder
public class ProductInfoFailureEvent {
    private String id;
    private String requestId;
    private String messageId;
    private int status;
    private String error;
    private String traceId;
}