package com.dobrev.auditservice.products.services;

import com.dobrev.auditservice.events.dto.ProductEventDto;
import com.dobrev.auditservice.events.dto.ProductEventType;
import com.dobrev.auditservice.events.dto.SnsMessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.List;

@Service
@Slf4j
public class ProductEventsConsumer {
    private final ObjectMapper objectMapper;
    private final SqsAsyncClient sqsAsyncClient;
    private final String productEventsQueueUrl;
    private final ReceiveMessageRequest receiveMessageRequest;

    public ProductEventsConsumer(ObjectMapper objectMapper,
                                 SqsAsyncClient sqsAsyncClient,
                                 @Value("${aws.sqs.queue.product.events.url}") String productEventsQueueUrl) {

        this.objectMapper = objectMapper;
        this.sqsAsyncClient = sqsAsyncClient;
        this.productEventsQueueUrl = productEventsQueueUrl;

        this.receiveMessageRequest = ReceiveMessageRequest.builder()
                .maxNumberOfMessages(5)
                .queueUrl(productEventsQueueUrl)
                .build();
    }

    @Scheduled(fixedDelay = 1000)
    public void receiveProductEventsMessages() {
        List<Message> messages;
        while (!(messages = sqsAsyncClient.receiveMessage(receiveMessageRequest).join().messages()).isEmpty()) {
            log.info("Reading {} messages", messages.size());
            messages.parallelStream().forEach(message -> {
                try {
                    SnsMessageDto snsMessageDto = objectMapper.readValue(message.body(), SnsMessageDto.class);
                    ThreadContext.put("messageId", snsMessageDto.messageId());
                    ThreadContext.put("requestId", snsMessageDto.messageAttributes().requestId().value());
                    ProductEventType eventType = ProductEventType
                            .valueOf(snsMessageDto.messageAttributes().eventType().value());

                    switch (eventType) {
                        case PRODUCT_CREATED, PRODUCT_UPDATED, PRODUCT_DELETED -> {
                            ProductEventDto productEventDto =
                                    objectMapper.readValue(snsMessageDto.message(), ProductEventDto.class);
                            log.info("Product event: {} - Id: {}", eventType, productEventDto.id());
                        }
                        default -> {
                            log.error("Invalid product event: {}", eventType);
                            throw new Exception("Invalid product event");
                        }
                    }

                    sqsAsyncClient.deleteMessage(DeleteMessageRequest.builder()
                            .queueUrl(productEventsQueueUrl)
                            .receiptHandle(message.receiptHandle())
                            .build()).join();
                    log.info("Message deleted...");
                } catch (Exception e) {
                    log.error("Failed to parse product event messsage");
                    throw new RuntimeException(e);
                } finally {
                    ThreadContext.clearAll();
                }
            });
        }
    }
}