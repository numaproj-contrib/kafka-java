package io.numaproj.confluent.kafka_sink;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class User {
    private String name;
    private int age;
}
