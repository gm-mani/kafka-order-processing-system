package com.mani.producer.model;


import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OrderRequest {

    @NotBlank(message = "orderId should not be null")
    private String orderId;

    @NotBlank(message = "product name should not be null")
    private String productName;

    @NotNull(message = "price should not be null")
    @Positive(message = "Negative values are not allowed for price")
    @Digits(integer = 10, fraction = 2, message = "price allows up to 10 digits and 2 decimals")
    private BigDecimal price;
}
