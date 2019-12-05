package com.kafka.orders;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Pair<T, K> {
    private T key;
    private K value;

    public static <T, K> Pair<T, K> of(T key, K value) {
        return new Pair<>(key, value);
    }
}
