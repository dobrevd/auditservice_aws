package com.dobrev.auditservice.products.controllers;

import com.amazonaws.xray.spring.aop.XRayEnabled;
import com.dobrev.auditservice.products.dto.ProductEventApiPageDto;
import com.dobrev.auditservice.products.services.ProductEventsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/products/events")
@RequiredArgsConstructor
@XRayEnabled
public class ProductEventsController {
    private final ProductEventsService productEventsService;

    @GetMapping
    public ProductEventApiPageDto getAll(
            @RequestParam String eventType,
            @RequestParam(defaultValue = "5") int limit,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String exclusiveStartTimeStamp
    ){
        return productEventsService.getAll(eventType, exclusiveStartTimeStamp, from, to, limit);
    }
}