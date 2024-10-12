package com.rulesengine.model;

import lombok.Data;
import lombok.Builder;

@Data
@Builder
public class RuleResult {
    private String messageId;
    private String reportDate;
    private String onlineStore;
    private String rpc;
    private String customerId;
    private String ruleName;
    private boolean rulePassed;
    private double ruleScore;
    private String errorMessage;
}