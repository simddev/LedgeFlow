package dev.simd.ledgeflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class LedgeFlowApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgeFlowApplication.class, args);
    }

}
