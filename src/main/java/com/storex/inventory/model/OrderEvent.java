package com.storex.inventory.model;

public record OrderEvent(
        String orderId,
        String productId,
        Integer quantity
) {
}

