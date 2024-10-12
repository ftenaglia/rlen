package com.rulesengine.service;


import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.rulesengine.model.RuleResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ResultPublisher {

    private final JdbcTemplate jdbcTemplate;
    private final AmazonS3 amazonS3Client;
    private KafkaConsumer<String, RuleResult> kafkaConsumer;

    @Value("${s3.bucket.name}")
    private String s3BucketName;

    @Value("${snowflake.table.name}")
    private String snowflakeTableName;

    @Value("${kafka.results.topic}")
    private String resultsTopic;

    @Scheduled(fixedRate = 300000) // Run every 5 minutes
    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public void publishResults() {
        List<RuleResult> batchToProcess = new ArrayList<>();

        // Poll the Kafka topic for all available messages
        while (true) {
            ConsumerRecords<String, RuleResult> records = kafkaConsumer.poll(Duration.ofMillis(100));
            if (records.isEmpty()) {
                break;
            }
            for (ConsumerRecord<String, RuleResult> record : records) {
                batchToProcess.add(record.value());
            }
        }

        if (!batchToProcess.isEmpty()) {
            String fileName = generateCsvFile(batchToProcess);
            String s3Key = uploadToS3(fileName);
            mergeIntoSnowflake(s3Key);

            // Commit the offsets after successful processing
            kafkaConsumer.commitSync();
        }
    }

    private String generateCsvFile(List<RuleResult> results) {
        String fileName = "rule_results_" + LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME) + ".csv";
        try (FileWriter out = new FileWriter(fileName);
             CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT
                     .withHeader("Report Date", "Online Store", "RPC", "Customer ID", "Rule Name", "Rule Pass", "Rule Score", "Error Message"))) {

            for (RuleResult result : results) {
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
            log.error("Error generating CSV file", e);
        }
        return fileName;
    }

    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private String uploadToS3(String fileName) {
        String s3Key = "rule-results/" + fileName;
        File file = new File(fileName);
        amazonS3Client.putObject(new PutObjectRequest(s3BucketName, s3Key, file));
        file.delete(); // Clean up local file after upload
        return s3Key;
    }

    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private void mergeIntoSnowflake(String s3Key) {
        String sql = String.format(
                "MERGE INTO %s t USING " +
                        "(SELECT $1 as report_date, $2 as online_store, $3 as rpc, $4 as customer_id, " +
                        "$5 as rule_name, $6 as rule_pass, $7 as rule_score, $8 as error_message " +
                        "FROM @%s/%s) s " +
                        "ON t.report_date = s.report_date AND t.rpc = s.rpc AND t.rule_name = s.rule_name " +
                        "WHEN MATCHED THEN UPDATE SET " +
                        "t.online_store = s.online_store, t.customer_id = s.customer_id, " +
                        "t.rule_pass = s.rule_pass, t.rule_score = s.rule_score, t.error_message = s.error_message " +
                        "WHEN NOT MATCHED THEN INSERT " +
                        "(report_date, online_store, rpc, customer_id, rule_name, rule_pass, rule_score, error_message) " +
                        "VALUES (s.report_date, s.online_store, s.rpc, s.customer_id, s.rule_name, s.rule_pass, s.rule_score, s.error_message)",
                snowflakeTableName, s3BucketName, s3Key
        );

        jdbcTemplate.execute(sql);
    }
}