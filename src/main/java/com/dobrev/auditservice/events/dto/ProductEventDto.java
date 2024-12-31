package com.dobrev.auditservice.events.dto;

import lombok.Builder;

@Builder
public record ProductEventDto(
    String id,
    String code,
    String email,
    float price
) { }