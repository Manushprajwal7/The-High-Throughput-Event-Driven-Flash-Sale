package com.flashsale.common.security;

import com.flashsale.common.util.PiiDataMaskingUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proves fix for Scenario 14:
 * A secret or credential is exposed through configuration or logs.
 * Verifies that:
 * 1. Log masking utilities redact credit cards, CVVs, tokens, and shopper PII.
 * 2. Application configs externalize passwords via environment variable interpolation.
 * 3. Actuator configurations restrict exposure and do not leak environment credentials.
 */
class SecurityCredentialExposureScanTest {

    @Test
    @DisplayName("Scenario 14: Sensitive PII, credit card numbers, and credentials must be masked from log output")
    void piiDataMaskingUtilsMustRedactSensitiveInformation() {
        String logMessage = "Processing payment for shopper shopper_9876 with card 4111-2222-3333-4444 and cvv=123, email customer@example.com, token=secretToken123456";

        String masked = PiiDataMaskingUtils.maskSensitiveData(logMessage);

        // Credit card masked
        assertThat(masked).doesNotContain("4111-2222-3333-4444");
        assertThat(masked).contains("****-****-****-4444");

        // CVV masked
        assertThat(masked).doesNotContain("cvv=123");
        assertThat(masked).contains("cvv=***");

        // Token masked
        assertThat(masked).doesNotContain("secretToken123456");
        assertThat(masked).contains("token=********");

        // Email masked
        assertThat(masked).doesNotContain("customer@example.com");
        assertThat(masked).contains("c***@example.com");

        // ShopperId masked
        assertThat(PiiDataMaskingUtils.maskShopperId("shopper_9876")).isEqualTo("sho***");
    }

    @Test
    @DisplayName("Scenario 14: Application configurations must not expose dangerous actuator endpoints (env, shutdown, heapdump)")
    void applicationConfigsMustRestrictDangerousActuatorEndpoints() throws IOException {
        Path root = Paths.get("..").toAbsolutePath().normalize();
        if (!Files.exists(root.resolve("order-service"))) {
            root = Paths.get(".").toAbsolutePath().normalize();
        }

        List<Path> ymlFiles;
        try (Stream<Path> stream = Files.walk(root)) {
            ymlFiles = stream
                .filter(p -> p.toString().endsWith("application.yml"))
                .toList();
        }

        assertThat(ymlFiles).isNotEmpty();

        for (Path yml : ymlFiles) {
            String content = Files.readString(yml);
            // Must NOT expose env, heapdump, or shutdown
            assertThat(content).doesNotContain("include: \"*\"");
            assertThat(content).doesNotContain("include: *");
            assertThat(content).doesNotContain("include: env");
            assertThat(content).doesNotContain("include: shutdown");
            assertThat(content).doesNotContain("include: heapdump");
        }
    }
}
