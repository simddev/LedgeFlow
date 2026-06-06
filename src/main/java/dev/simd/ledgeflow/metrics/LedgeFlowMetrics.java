package dev.simd.ledgeflow.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class LedgeFlowMetrics {

    private final Counter depositCounter;
    private final Counter withdrawalCounter;
    private final Counter transferCounter;
    private final Counter alertCounter;
    private final DistributionSummary transactionAmountSummary;
    private final Timer rebuildTimer;

    public LedgeFlowMetrics(MeterRegistry registry) {
        this.depositCounter = Counter.builder("ledgeflow.transactions.total")
                .tag("type", "deposit")
                .register(registry);

        this.withdrawalCounter = Counter.builder("ledgeflow.transactions.total")
                .tag("type", "withdrawal")
                .register(registry);

        this.transferCounter = Counter.builder("ledgeflow.transactions.total")
                .tag("type", "transfer")
                .register(registry);

        this.alertCounter = Counter.builder("ledgeflow.alerts.total")
                .register(registry);

        this.transactionAmountSummary = DistributionSummary.builder("ledgeflow.transactions.amount")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);

        this.rebuildTimer = Timer.builder("ledgeflow.admin.rebuild.duration")
                .register(registry);
    }

    public void incrementDeposit(BigDecimal amount) {
        depositCounter.increment();
        transactionAmountSummary.record(amount.doubleValue());
    }

    public void incrementWithdrawal(BigDecimal amount) {
        withdrawalCounter.increment();
        transactionAmountSummary.record(amount.doubleValue());
    }

    public void incrementTransfer(BigDecimal amount) {
        transferCounter.increment();
        transactionAmountSummary.record(amount.doubleValue());
    }

    public void incrementAlert() {
        alertCounter.increment();
    }

    public Timer getRebuildTimer() {
        return rebuildTimer;
    }
}
