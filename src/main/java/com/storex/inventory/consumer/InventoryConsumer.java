package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderEvent;
import com.storex.inventory.service.InventoryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    private final InventoryService inventoryService;

    public InventoryConsumer(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(
            topics = "${storex.kafka.order-topic}",
            groupId = "${spring.kafka.consumer.group-id}")
    public void consume(OrderEvent event) {
        log.info("Received order event: orderId={}, productId={}, quantity={}",
                event.orderId(), event.productId(), event.quantity());

        inventoryService.deductStock(event.productId(), event.quantity());
    }
}

