package com.dobrev.auditservice.products.services;

import com.amazonaws.xray.spring.aop.XRayEnabled;
import com.dobrev.auditservice.products.dto.ProductEventApiDto;
import com.dobrev.auditservice.products.dto.ProductEventApiPageDto;
import com.dobrev.auditservice.products.models.ProductEvent;
import com.dobrev.auditservice.products.repositories.ProductEventsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.enhanced.dynamodb.model.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

@Service
@XRayEnabled
@RequiredArgsConstructor
public class ProductEventsService {
    private final ProductEventsRepository productEventsRepository;

    public ProductEventApiPageDto getAll(String eventType, String exclusiveStartTimeStamp,
                                         String from, String to, int limit){
        List<ProductEventApiDto> productEventApiDtoList = new ArrayList<>();

        SdkPublisher<Page<ProductEvent>> productEventsPublisher = (from != null && to != null) ?
                productEventsRepository.findByTypeAndRange(eventType, exclusiveStartTimeStamp, from, to, limit)
                : productEventsRepository.findByType(eventType, exclusiveStartTimeStamp, limit);

        AtomicReference<String> lastEvaluatedTimeStamp = new AtomicReference<>();
        productEventsPublisher.subscribe(productEventPage -> {
            productEventApiDtoList.addAll(productEventPage.items().stream().map(ProductEventApiDto::new).toList());
            if (productEventPage.lastEvaluatedKey() != null){
                lastEvaluatedTimeStamp.set(productEventPage.lastEvaluatedKey().get("sk").s());
            }
        }).join();

        return new ProductEventApiPageDto(productEventApiDtoList, lastEvaluatedTimeStamp.get(), productEventApiDtoList.size());
    }
}