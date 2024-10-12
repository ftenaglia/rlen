package com.rulesengine.model;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class InputMessage {
    private String messageId;
    private String tableName;
    private int totalRpcCount;
}