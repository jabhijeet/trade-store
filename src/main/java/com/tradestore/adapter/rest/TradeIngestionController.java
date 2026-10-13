package com.tradestore.adapter.rest;

import com.tradestore.domain.EventSource;
import com.tradestore.domain.SaveOutcome;
import com.tradestore.domain.TradeCommand;
import com.tradestore.service.TradeProcessingService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

/**
 * Backup and direct ingestion path. Writes through the same processing service as the Kafka consumer and does
 * not depend on Kafka.
 */
@RestController
@RequestMapping("/api/v1/trades")
@RequiredArgsConstructor
public class TradeIngestionController {

    static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private final TradeProcessingService tradeProcessingService;

    /**
     * Stores a trade. Returns {@code 201} for a new trade id and {@code 200} for a higher or same version.
     * The message id is {@code rest:<Idempotency-Key>}, or {@code rest:<random UUID>} when no key is sent.
     */
    @PostMapping
    public ResponseEntity<IngestionResponse> ingestTrade(
            @Valid @RequestBody TradeRequest request,
            @RequestHeader(name = IDEMPOTENCY_KEY_HEADER, required = false) @Size(max = 200) String idempotencyKey) {
        String key = idempotencyKey == null || idempotencyKey.isBlank()
                ? UUID.randomUUID().toString()
                : idempotencyKey;
        SaveOutcome outcome = tradeProcessingService.process(
                new TradeCommand("rest:" + key, EventSource.REST, request.toIncomingTrade()));

        var body = new IngestionResponse(request.tradeId(), request.version(), outcome);
        if (outcome == SaveOutcome.CREATED) {
            URI location = ServletUriComponentsBuilder.fromCurrentRequest()
                    .path("/{tradeId}")
                    .buildAndExpand(request.tradeId())
                    .toUri();
            return ResponseEntity.created(location).body(body);
        }
        return ResponseEntity.ok(body);
    }
}
