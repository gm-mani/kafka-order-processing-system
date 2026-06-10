package com.mani.producer.Controller;


import com.mani.producer.Service.OrderService;
import com.mani.producer.model.OrderRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("/api")
@Slf4j
public class KafkaProducer {

    private final OrderService orderService;

    public KafkaProducer(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping("/createOrder")
    public ResponseEntity<String> createOrder(
            @RequestBody OrderRequest request
    ) {
        log.info("Received order creation request for product: {}", request.getProductName());
        orderService.createOrder(request);
        log.info("Order created successfully");
        return ResponseEntity.ok().body("Order created");
    }

}