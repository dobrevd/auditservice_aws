package com.dobrev.auditservice.events.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;

@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProductFailureEventDto(
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String id,
        String error,
        String email,
        int status
) {}