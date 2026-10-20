package com.flashsale.payment.simulation;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/simulation/payment")
public class PaymentSimulationController {

    private final PaymentSimulationService simulationService;

    public PaymentSimulationController(PaymentSimulationService simulationService) {
        this.simulationService = simulationService;
    }

    @GetMapping
    public ResponseEntity<Map<String, String>> getSimulationMode() {
        return ResponseEntity.ok(Map.of(
            "mode", simulationService.getMode().name(),
            "description", "Simulated payment outcome configuration for test/demo environments"
        ));
    }

    @PutMapping
    public ResponseEntity<Map<String, String>> setSimulationMode(@RequestBody Map<String, String> request) {
        String modeStr = request.get("mode");
        if (modeStr == null || modeStr.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "mode field is required"));
        }
        try {
            PaymentSimulationMode mode = PaymentSimulationMode.valueOf(modeStr.toUpperCase());
            simulationService.setMode(mode);
            return ResponseEntity.ok(Map.of(
                "mode", simulationService.getMode().name(),
                "status", "UPDATED"
            ));
        } catch (IllegalArgumentException ex) {
            return ResponseEntity.badRequest().body(Map.of(
                "error", "Invalid mode: " + modeStr + ". Allowed values: ALWAYS_SUCCESS, ALWAYS_DECLINE, ALWAYS_TIMEOUT, RULE_BASED"
            ));
        }
    }
}
