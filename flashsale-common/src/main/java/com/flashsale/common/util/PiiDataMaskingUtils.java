package com.flashsale.common.util;

import java.util.regex.Pattern;

/**
 * Utility for masking Personally Identifiable Information (PII) and credentials in logs.
 * (Scenario 14 / Security Hardening)
 */
public final class PiiDataMaskingUtils {

    private static final Pattern CARD_PATTERN = Pattern.compile("\\b(?:\\d{4}[- ]?){3}(\\d{4})\\b");
    private static final Pattern CVV_PATTERN = Pattern.compile("(?i)\\b(cvv|cvc|securityCode)[\"']?\\s*[:=]\\s*[\"']?(\\d{3,4})\\b");
    private static final Pattern TOKEN_PATTERN = Pattern.compile("(?i)\\b(password|secret|bearer|token)[\"']?\\s*[:=]\\s*[\"']?([^\"'\\s,]{6,})\\b");
    private static final Pattern EMAIL_PATTERN = Pattern.compile("(?i)\\b([a-zA-Z0-9_.+-])[a-zA-Z0-9_.+-]*@([a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+)\\b");

    private PiiDataMaskingUtils() {}

    /**
     * Masks credit cards, CVVs, tokens, passwords, and emails in logging text.
     */
    public static String maskSensitiveData(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }

        // Mask credit cards: ****-****-****-1234
        String masked = CARD_PATTERN.matcher(input).replaceAll("****-****-****-$1");

        // Mask CVV
        masked = CVV_PATTERN.matcher(masked).replaceAll("$1=***");

        // Mask passwords and tokens
        masked = TOKEN_PATTERN.matcher(masked).replaceAll("$1=********");

        // Mask emails: j***@domain.com
        masked = EMAIL_PATTERN.matcher(masked).replaceAll("$1***@$2");

        return masked;
    }

    /**
     * Masks shopper IDs for log compliance (e.g., shopper_12345 -> shop***).
     */
    public static String maskShopperId(String shopperId) {
        if (shopperId == null || shopperId.length() <= 3) {
            return "***";
        }
        return shopperId.substring(0, 3) + "***";
    }
}
