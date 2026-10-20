package com.flashsale.order.util;

import com.flashsale.common.dto.CreateOrderRequest;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class RequestFingerprintUtilsTest {

    @Test
    void shouldProduceDeterministicFingerprint() {
        UUID saleId = UUID.randomUUID();
        CreateOrderRequest req1 = new CreateOrderRequest(saleId, "shopper_1", 1);
        CreateOrderRequest req2 = new CreateOrderRequest(saleId, "shopper_1", 1);

        String fp1 = RequestFingerprintUtils.computeFingerprint(req1);
        String fp2 = RequestFingerprintUtils.computeFingerprint(req2);

        assertThat(fp1).isEqualTo(fp2);
        assertThat(fp1).hasSize(64); // SHA-256 hex string length
    }

    @Test
    void shouldProduceDifferentFingerprintsForDifferentPayloads() {
        UUID saleId = UUID.randomUUID();
        CreateOrderRequest req1 = new CreateOrderRequest(saleId, "shopper_1", 1);
        CreateOrderRequest req2 = new CreateOrderRequest(saleId, "shopper_1", 2);
        CreateOrderRequest req3 = new CreateOrderRequest(saleId, "shopper_2", 1);

        String fp1 = RequestFingerprintUtils.computeFingerprint(req1);
        String fp2 = RequestFingerprintUtils.computeFingerprint(req2);
        String fp3 = RequestFingerprintUtils.computeFingerprint(req3);

        assertThat(fp1).isNotEqualTo(fp2);
        assertThat(fp1).isNotEqualTo(fp3);
    }
}
