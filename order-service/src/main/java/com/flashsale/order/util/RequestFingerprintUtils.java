package com.flashsale.order.util;

import com.flashsale.common.dto.CreateOrderRequest;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

public final class RequestFingerprintUtils {
    private RequestFingerprintUtils() {}

    public static String computeFingerprint(CreateOrderRequest request) {
        String canonical = String.format("%s:%s:%d",
            request.saleId(),
            request.shopperId().trim(),
            request.quantity()
        );

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }
}
