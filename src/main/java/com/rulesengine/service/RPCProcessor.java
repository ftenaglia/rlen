package com.rulesengine.service;

import com.rulesengine.model.Product;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Slf4j
@Service
@RequiredArgsConstructor
public class RPCProcessor {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final JdbcTemplate jdbcTemplate;
    private final ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    public void processNewRPCTable(String tableName) {
        try {
            int batchSize = 1000;
            int offset = 0;

            while (true) {
                List<Product> products = fetchProductsFromTable(tableName, offset, batchSize);
                if (products.isEmpty()) {
                    break;
                }

                offset += batchSize;

                CompletableFuture.runAsync(() -> processProductBatch(products), executorService)
                        .exceptionally(e -> {
                            log.error("Error processing product batch", e);
                            return null;
                        });
            }
        } catch (Exception e) {
            log.error("Error processing new RPC table: {}", e.getMessage(), e);
            throw e; // Rethrow for retry
        }
    }

    private void processProductBatch(List<Product> products) {
        for (Product product : products) {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send("rpc-for-processing", product);
            future.thenAccept(result -> log.debug("Sent product {} to Kafka", product.getRpc()))
                    .exceptionally(ex -> {
                        log.error("Error sending product {} to Kafka", product.getRpc(), ex);
                        return null;
                    });
        }
    }

    @Retryable(value = {Exception.class}, maxAttempts = 3, backoff = @Backoff(delay = 1000))
    private List<Product> fetchProductsFromTable(String tableName, int offset, int limit) {
        String sql = "SELECT * FROM " + tableName + " LIMIT ? OFFSET ?";
        return jdbcTemplate.query(sql, new Object[]{limit, offset}, (rs, rowNum) -> {
                    Product product = new Product();
                    product.setRpc(rs.getString("rpc"));
                    product.setClientId(rs.getString("client_id"));
                    product.setRetailer(rs.getString("retailer"));
                    product.setBrand(rs.getString("brand"));
                    product.setCategory(rs.getString("category"));

                    Map<String, String> attributes = new HashMap<>();
                    for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                        String columnName = rs.getMetaData().getColumnName(i);
                        if (!List.of("rpc", "client_id", "retailer", "brand", "category").contains(columnName)) {
                            attributes.put(columnName, rs.getString(columnName));
                        }
                    }
                    product.setAttributes(attributes);

                    return product;
                }
        );
    }
}