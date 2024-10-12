package com.rulesengine.rule;

import com.rulesengine.model.Product;
import com.rulesengine.model.RuleConfig;
import com.rulesengine.model.RuleResult;
import com.rulesengine.util.RuleConfigurationLoader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;

@Component
@RequiredArgsConstructor
public class TitleLengthRule implements Rule {

    private final RuleConfigurationLoader configLoader;

    @Override
    public RuleResult apply(Product product) {
        RuleConfig config = configLoader.loadConfig("TitleLengthRule");
        String title = product.getAttributes().get("title");
        int minLength = Integer.parseInt(config.getParameters().get("minLength"));
        int maxLength = Integer.parseInt(config.getParameters().get("maxLength"));

        boolean passed = title != null && title.length() >= minLength && title.length() <= maxLength;
        double score = passed ? 1.0 : 0.0;
        String errorMessage = passed ? "" : "Title length is not within the specified range";

        return RuleResult.builder()
                .messageId(product.getMessageId())
                .reportDate(LocalDate.now().toString())
                .onlineStore(product.getRetailer())
                .rpc(product.getRpc())
                .customerId(product.getClientId())
                .ruleName("TitleLengthRule")
                .rulePassed(passed)
                .ruleScore(score)
                .errorMessage(errorMessage)
                .build();
    }

    @Override
    public boolean isApplicable(Product product) {
        RuleConfig config = configLoader.loadConfig("TitleLengthRule");
        return isApplicableTo(config.getApplicableTo(), "retailer", product.getRetailer()) &&
                isApplicableTo(config.getApplicableTo(), "brand", product.getBrand()) &&
                isApplicableTo(config.getApplicableTo(), "category", product.getCategory()) &&
                !isApplicableTo(config.getExclusions(), "retailer", product.getRetailer()) &&
                !isApplicableTo(config.getExclusions(), "brand", product.getBrand()) &&
                !isApplicableTo(config.getExclusions(), "category", product.getCategory());
    }

    private boolean isApplicableTo(Map<String, List<String>> applicableTo, String key, String value) {
        return applicableTo.containsKey(key) && applicableTo.get(key).contains(value);
    }
}