package com.storex.inventory.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    public void deductStock(String productId, Integer quantity) {
        if (productId == null || quantity == null || quantity <= 0) {
            throw new IllegalArgumentException("Du lieu tru kho khong hop le");
        }

        log.info("Deduct stock: productId={}, quantity={}", productId, quantity);
    }
}

