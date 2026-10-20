package com.flashsale.gateway;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@ActiveProfiles("test")
class DashboardStaticContentIntegrationTest {

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void shouldServeOperatorDashboardIndexHtml() {
        webTestClient.get()
            .uri("/index.html")
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentTypeCompatibleWith(MediaType.TEXT_HTML)
            .expectBody(String.class)
            .consumeWith(response -> {
                String body = response.getResponseBody();
                org.assertj.core.api.Assertions.assertThat(body)
                    .contains("FlashSale Engine")
                    .contains("Operator Console")
                    .contains("Purchase Simulator")
                    .contains("Payment Simulation & Failure Controls");
            });
    }
}
