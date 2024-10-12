package com.rulesengine.service;

import com.rulesengine.model.RuleResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResultAggregator {

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, List<RuleResult>>> messageResults = new ConcurrentHashMap<>();

    // Highlight: Changed to accept a list of RuleResults
    @KafkaListener(topics = "rule-results", groupId = "result-aggregator-group", concurrency = "5")
    public void aggregateResults(List<RuleResult> results) {
        for (RuleResult result : results) {
            messageResults
                    .computeIfAbsent(result.getMessageId(), k -> new ConcurrentHashMap<>())
                    .computeIfAbsent(result.getRpc(), k -> new ArrayList<>())
                    .add(result);
        }
    }

    @KafkaListener(topics = "processing-complete", groupId = "result-aggregator-group")
    public void generateCsvFile(String messageId) {
        String fileName = "rule_results_" + messageId + ".csv";

        // Highlight: Use try-with-resources for proper resource management
        try (FileWriter out = new FileWriter(fileName);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                     .withHeader("Report Date", "Online Store", "RPC", "Customer ID", "Rule Name", "Rule Pass", "Rule Score", "Error Message"))) {

            ConcurrentHashMap<String, List<RuleResult>> results = messageResults.get(messageId);
            if (results != null) {
                // Highlight: Process results in parallel
                results.values().parallelStream().forEach(rpcResults -> {
                    for (RuleResult result : rpcResults) {
                        try {
                            synchronized (printer) {
                                printer.printRecord(
                                        result.getReportDate(),
                                        result.getOnlineStore(),
                                        result.getRpc(),
                                        result.getCustomerId(),
                                        result.getRuleName(),
                                        result.isRulePassed(),
                                        result.getRuleScore(),
                                        result.getErrorMessage()
                                );
                            }
                        } catch (IOException e) {
                            log.error("Error writing to CSV file", e);
                        }
                    }
                });
            }
            log.info("CSV file generated for message {}: {}", messageId, fileName);
        } catch (IOException e) {
            log.error("Error generating CSV file for message {}", messageId, e);
        } finally {
            messageResults.remove(messageId);
        }
    }
}