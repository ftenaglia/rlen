package com.rulesengine.service;

import com.rulesengine.model.InputMessage;
import com.rulesengine.model.Product;
import com.rulesengine.model.RuleResult;
import com.rulesengine.rule.Rule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class RuleEngine {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final List<Rule> rules;
    private final ClientConfigService clientConfigService;

    @KafkaListener(topics = "rpc-for-processing", groupId = "rule-engine-group", concurrency = "10")
    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public void processRPC(Product product) {
        try {
            List<Rule> enabledRules = clientConfigService.getEnabledRules(product.getClientId());

            List<RuleResult> results = enabledRules.parallelStream()
                    .filter(rule -> rule.isApplicable(product))
                    .map(rule -> rule.apply(product))
                    .toList();

            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("rule-results", results);
            future.thenAccept(result -> log.debug("Sent rule results for product {}", product.getRpc()))
                    .exceptionally(ex -> {
                        log.error("Error sending rule results for product {}", product.getRpc(), ex);
                        return null;
                    });
        } catch (Exception e) {
            log.error("Error processing RPC: {}", e.getMessage(), e);
            throw e; // Rethrow for retry
        }
    }
}