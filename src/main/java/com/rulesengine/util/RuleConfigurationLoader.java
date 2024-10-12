package com.rulesengine.util;

import com.rulesengine.model.RuleConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class RuleConfigurationLoader {

    private final RedisTemplate<String, Object> redisTemplate;

    public RuleConfig loadConfig(String ruleName) {
        return (RuleConfig) redisTemplate.opsForValue().get("rule_config:" + ruleName);
    }

    public List<String> getEnabledRules(String clientId) {
        return (List<String>) redisTemplate.opsForValue().get("enabled_rules:" + clientId);
    }
}
