package com.dobrev.auditservice.events.dto;

public record SnsAttribute(
        SnsMessageAttribute traceId,
        SnsMessageAttribute eventType,
        SnsMessageAttribute requestId
) {}