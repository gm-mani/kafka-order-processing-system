package com.mani.producer.controller;


import com.mani.producer.model.OrderRequest;
import com.mani.producer.service.OrderService;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Slf4j
public class KafkaProducer {

    private final OrderService orderService;

    public KafkaProducer(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/createOrder")
    public ResponseEntity<String> createOrder(
            @Valid @RequestBody OrderRequest request
    ) {
        log.info("Received order creation request for product: {}", request.getProductName());
        orderService.createOrder(request);
        log.info("Order created successfully");
        return ResponseEntity.ok().body("Order created");
    }

}