package dev.simd.ledgeflow;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"account.events", "account.alerts"})
class LedgeFlowApplicationTests {

    @Test
    void contextLoads() {
    }

}
