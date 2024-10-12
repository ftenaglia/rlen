package com.rulesengine.model;

import lombok.Data;

import java.util.Map;

@Data
public class Product {
    private String messageId;
    private String rpc;
    private String clientId;
    private String retailer;
    private String brand;
    private String category;
    private Map<String, String> attributes;
}