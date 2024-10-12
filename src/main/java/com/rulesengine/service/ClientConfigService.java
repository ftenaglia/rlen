package com.rulesengine.service;

import com.rulesengine.rule.Rule;
import com.rulesengine.util.RuleConfigurationLoader;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ClientConfigService {

    private final List<Rule> allRules;
    private final RuleConfigurationLoader ruleConfigurationLoader;

    public List<Rule> getEnabledRules(String clientId) {
        List<String> enabledRuleNames = ruleConfigurationLoader.getEnabledRules(clientId);
        return allRules.stream()
                .filter(rule -> enabledRuleNames.contains(rule.getClass().getSimpleName()))
                .collect(Collectors.toList());
    }
}
