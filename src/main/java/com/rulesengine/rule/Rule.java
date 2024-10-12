package com.rulesengine.rule;

import com.rulesengine.model.Product;
import com.rulesengine.model.RuleResult;

public interface Rule {
    RuleResult apply(Product product);
    default boolean isApplicable(Product product) {

    }
}
