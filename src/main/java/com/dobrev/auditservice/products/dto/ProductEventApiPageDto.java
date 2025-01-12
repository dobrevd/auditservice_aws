package com.dobrev.auditservice.products.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

public record ProductEventApiPageDto(
        List<ProductEventApiDto> items,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        String lastEvaluatedTimeStamp,
        int count
) { }
