package com.kafka.orders;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String id;
    private Long customerId;
    private State state;
    private Product product;
    private Integer quantity;
    private BigDecimal price;

    public enum State {
        CREATED, VALIDATED, FAILED, SHIPPED
    }
}
