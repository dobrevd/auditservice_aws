package com.dobrev.auditservice.events.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SnsMessageDto(
        @JsonProperty("Message")
        String message,
        @JsonProperty("Type")
        String type,
        @JsonProperty("TopicArn")
        String topicArn,
        @JsonProperty("Timestamp")
        String timestamp,
        @JsonProperty("MessageId")
        String messageId,
        @JsonProperty("MessageAttributes")
        SnsAttribute messageAttributes
) {}
