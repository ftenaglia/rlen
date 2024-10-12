package com.rulesengine.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class RuleConfig {
    private String ruleName;
    private Map<Dimension, List<String>> applicableTo;
    private Map<Dimension, List<String>> exclusions;
    private Map<String, String> parameters;
}