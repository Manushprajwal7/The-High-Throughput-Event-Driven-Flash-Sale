package com.flashsale.payment;

import com.flashsale.payment.simulation.PaymentResult;
import com.flashsale.payment.simulation.PaymentSimulationService;
import com.flashsale.payment.simulation.PaymentTimeoutException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves fix for Scenario 11 (REM-10):
 * Downstream payment gateway / 3rd party service backpressure.
 * When workers scale faster than downstream capacity, the concurrency limiter
 * bounds in-flight calls and throws PaymentTimeoutException under saturation,
 * allowing Kafka/SQS retry backoff to pace work safely without overwhelming the downstream provider.
 */
class DownstreamConsumerBackpressureTest {

    @Test
    @DisplayName("Scenario 11 (REM-10): Concurrency limiter applies backpressure when worker concurrency exceeds downstream capacity")
    void concurrencyLimiterEnforcesBackpressureUnderBurst() throws Exception {
        int maxDownstreamConcurrency = 2;
        long simulatedLatencyMs = 200;

        PaymentSimulationService simulationService = new PaymentSimulationService(
            "ALWAYS_SUCCESS",
            simulatedLatencyMs,
            maxDownstreamConcurrency
        );

        int burstRequests = 6;
        ExecutorService executor = Executors.newFixedThreadPool(burstRequests);
        CyclicBarrier barrier = new CyclicBarrier(burstRequests);
        CountDownLatch latch = new CountDownLatch(burstRequests);

        AtomicInteger successfulCalls = new AtomicInteger(0);
        AtomicInteger backpressuredCalls = new AtomicInteger(0);

        for (int i = 0; i < burstRequests; i++) {
            executor.submit(() -> {
                try {
                    barrier.await();
                    PaymentResult result = simulationService.processPayment(UUID.randomUUID(), "shopper_burst", 1);
                    if (result.successful()) {
                        successfulCalls.incrementAndGet();
                    }
                } catch (PaymentTimeoutException ex) {
                    backpressuredCalls.incrementAndGet();
                } catch (Exception e) {
                    // Ignore barrier exceptions
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await(3, TimeUnit.SECONDS);
        executor.shutdown();

        // INVARIANT: At least some calls were backpressured because burst (6) >> maxConcurrency (2)
        assertThat(backpressuredCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(successfulCalls.get()).isGreaterThanOrEqualTo(1);
        assertThat(successfulCalls.get() + backpressuredCalls.get()).isEqualTo(burstRequests);
    }
}
