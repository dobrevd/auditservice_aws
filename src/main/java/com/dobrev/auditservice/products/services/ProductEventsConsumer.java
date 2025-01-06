package com.dobrev.auditservice.products.services;

import com.amazonaws.xray.AWSXRay;
import com.amazonaws.xray.entities.Segment;
import com.amazonaws.xray.entities.TraceID;
import com.dobrev.auditservice.events.dto.ProductEventDto;
import com.dobrev.auditservice.events.dto.ProductEventType;
import com.dobrev.auditservice.events.dto.SnsMessageDto;
import com.dobrev.auditservice.products.repositories.ProductEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class ProductEventsConsumer {
    private final ObjectMapper objectMapper;
    private final SqsAsyncClient sqsAsyncClient;
    private final String productEventsQueueUrl;
    private final ReceiveMessageRequest receiveMessageRequest;
    private final ProductEventRepository productEventRepository;

    public ProductEventsConsumer(ObjectMapper objectMapper,
                                 SqsAsyncClient sqsAsyncClient,
                                 @Value("${aws.sqs.queue.product.events.url}") String productEventsQueueUrl,
                                 ProductEventRepository productEventRepository) {

        this.objectMapper = objectMapper;
        this.sqsAsyncClient = sqsAsyncClient;
        this.productEventsQueueUrl = productEventsQueueUrl;

        this.receiveMessageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(5)
                .queueUrl(productEventsQueueUrl)
                .build();
        this.productEventRepository = productEventRepository;
    }

    @Scheduled(fixedDelay = 1000)
    public void receiveProductEventsMessages() {
        List<Message> messages;
        while (!(messages = sqsAsyncClient.receiveMessage(receiveMessageRequest).join().messages()).isEmpty()) {
            log.info("Reading {} messages", messages.size());
            messages.parallelStream().forEach(message -> {
                SnsMessageDto snsMessageDto;
                try {
                    snsMessageDto = objectMapper.readValue(message.body(), SnsMessageDto.class);
                } catch (JsonProcessingException e){
                    throw new RuntimeException(e);
                }

                String requestId = snsMessageDto.messageAttributes().requestId().value();
                String messageId = snsMessageDto.messageId();
                String traceId = snsMessageDto.messageAttributes().traceId().value();

                Segment segment = AWSXRay.beginSegment("product-events-consumer");
                segment.setOrigin("AWS::ECS::Container");
                segment.setStartTime(Instant.now().getEpochSecond());
                segment.setTraceId(TraceID.fromString(traceId));
                segment.run(() -> {
                    try {
                        ThreadContext.put("messageId", messageId);
                        ThreadContext.put("requestId", requestId);
                        ProductEventType eventType = ProductEventType
                                .valueOf(snsMessageDto.messageAttributes().eventType().value());

                        CompletableFuture<Void> productEventFuture;
                        switch (eventType) {
                            case PRODUCT_CREATED, PRODUCT_UPDATED, PRODUCT_DELETED -> {
                                ProductEventDto productEventDto =
                                        objectMapper.readValue(snsMessageDto.message(), ProductEventDto.class);

                                productEventFuture = productEventRepository.create(productEventDto, eventType,
                                        messageId, requestId, traceId);

                                log.info("Product event: {} - Id: {}", eventType, productEventDto.id());
                            }
                            default -> {
                                log.error("Invalid product event: {}", eventType);
                                throw new Exception("Invalid product event");
                            }
                        }

                        CompletableFuture<DeleteMessageResponse> deleteMessageCompletableFuture =
                                sqsAsyncClient.deleteMessage(DeleteMessageRequest.builder()
                                        .queueUrl(productEventsQueueUrl)
                                        .receiptHandle(message.receiptHandle())
                                        .build());

                        CompletableFuture.allOf(productEventFuture, deleteMessageCompletableFuture).join();

                        log.info("Message deleted...");
                    } catch (Exception e) {
                        log.error("Failed to parse product event message");
                        throw new RuntimeException(e);
                    } finally {
                        ThreadContext.clearAll();
                        segment.setEndTime(Instant.now().getEpochSecond());
                        segment.end();
                        segment.close();
                    }
                }, AWSXRay.getGlobalRecorder());
            });
        }
        AWSXRay.endSegment();
    }
}