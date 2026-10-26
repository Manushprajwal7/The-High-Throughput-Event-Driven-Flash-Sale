package com.flashsale.payment.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class PaymentSimulationService {

    private static final Logger log = LoggerFactory.getLogger(PaymentSimulationService.class);

    private final AtomicReference<PaymentSimulationMode> currentMode;
    private final long latencyMs;
    private final Semaphore concurrencyLimiter;
    private final Map<UUID, String> capturedPayments = new ConcurrentHashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    public PaymentSimulationService(
        @Value("${flashsale.simulation.payment-mode:ALWAYS_SUCCESS}") String initialMode,
        @Value("${flashsale.simulation.latency-ms:0}") long latencyMs,
        @Value("${flashsale.downstream.max-concurrency:20}") int maxConcurrency
    ) {
        PaymentSimulationMode mode;
        try {
            mode = PaymentSimulationMode.valueOf(initialMode.toUpperCase());
        } catch (Exception ex) {
            mode = PaymentSimulationMode.ALWAYS_SUCCESS;
        }
        this.currentMode = new AtomicReference<>(mode);
        this.latencyMs = latencyMs;
        this.concurrencyLimiter = new Semaphore(Math.max(1, maxConcurrency));
        log.info("Initialized PaymentSimulationService with mode: {}, latency: {}ms, maxConcurrency: {}", mode, latencyMs, maxConcurrency);
    }

    public PaymentSimulationService(
        String initialMode,
        long latencyMs
    ) {
        this(initialMode, latencyMs, 20);
    }

    public PaymentSimulationMode getMode() {
        return currentMode.get();
    }

    public void setMode(PaymentSimulationMode mode) {
        log.info("Switching payment simulation mode from {} to {}", currentMode.get(), mode);
        this.currentMode.set(mode);
    }

    public PaymentResult processPayment(UUID orderId, String shopperId, int quantity) {
        boolean acquired = false;
        try {
            acquired = concurrencyLimiter.tryAcquire(200, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (!acquired) {
            log.warn("Downstream payment concurrency limit reached for order {}. Applying backpressure.", orderId);
            throw new PaymentTimeoutException("Downstream payment gateway backpressure limit reached for order " + orderId);
        }

        try {
            if (latencyMs > 0) {
                try {
                    Thread.sleep(latencyMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            PaymentSimulationMode mode = currentMode.get();

            switch (mode) {
                case ALWAYS_DECLINE -> {
                    log.info("Payment simulation DECLINED order {} (ALWAYS_DECLINE mode)", orderId);
                    return PaymentResult.declined("INSUFFICIENT_FUNDS", "Payment declined by simulated issuing bank");
                }
                case ALWAYS_TIMEOUT -> {
                    log.warn("Payment simulation TIMEOUT for order {} (ALWAYS_TIMEOUT mode)", orderId);
                    throw new PaymentTimeoutException("Simulated payment gateway timeout for order: " + orderId);
                }
                case RULE_BASED -> {
                    String normalizedShopper = shopperId != null ? shopperId.toLowerCase() : "";
                    if (normalizedShopper.contains("decline") || normalizedShopper.contains("fail")) {
                        log.info("Payment simulation DECLINED order {} (RULE_BASED: shopper '{}')", orderId, shopperId);
                        return PaymentResult.declined("CARD_DECLINED", "Simulated card decline for shopper");
                    }
                    if (normalizedShopper.contains("timeout")) {
                        log.warn("Payment simulation TIMEOUT for order {} (RULE_BASED: shopper '{}')", orderId, shopperId);
                        throw new PaymentTimeoutException("Simulated payment timeout for shopper: " + shopperId);
                    }
                    String ref = "PAY-SIM-" + UUID.randomUUID();
                    capturedPayments.put(orderId, ref);
                    log.info("Payment simulation SUCCESS for order {} (RULE_BASED), ref: {}", orderId, ref);
                    return PaymentResult.success(ref);
                }
                case ALWAYS_SUCCESS -> {
                    String ref = "PAY-SIM-" + UUID.randomUUID();
                    capturedPayments.put(orderId, ref);
                    log.info("Payment simulation SUCCESS for order {} (ALWAYS_SUCCESS), ref: {}", orderId, ref);
                    return PaymentResult.success(ref);
                }
                default -> throw new IllegalStateException("Unexpected payment simulation mode: " + mode);
            }
        } finally {
            concurrencyLimiter.release();
        }
    }

    public boolean isPaymentCaptured(UUID orderId) {
        return capturedPayments.containsKey(orderId);
    }

    public String getPaymentReference(UUID orderId) {
        return capturedPayments.get(orderId);
    }

    public boolean refundPayment(String paymentReference) {
        log.info("Payment simulation REFUNDED payment ref: {}", paymentReference);
        capturedPayments.values().removeIf(ref -> ref.equals(paymentReference));
        return true;
    }
}
