package com.rulesengine.service;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@ConditionalOnProperty(name = "app.role", havingValue = "rpc-listener")
public class RPCListener {

    private final RPCProcessor rpcProcessor;

    public RPCListener(RPCProcessor rpcProcessor) {
        this.rpcProcessor = rpcProcessor;
    }

    @KafkaListener(topics = "new-rpc-tables", groupId = "rpc-listener-group")
    public void listenNewRPCTables(String tableName) {
        rpcProcessor.processNewRPCTable(tableName);
    }
}
