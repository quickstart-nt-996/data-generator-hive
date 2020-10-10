package com.quickstart.dao;

import com.quickstart.model.JdbcConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;

@Slf4j
public class JdbcTemplateFactory {

    private static HashMap<String, JdbcTemplate> jdbcTemplateMap = new HashMap<>();

    public static void setConfigs(JdbcConfig jdbcConfig) {
        JdbcTemplate mysqlDao = new JdbcTemplate(jdbcConfig);
        jdbcTemplateMap.put(jdbcConfig.getName(), mysqlDao);
    }


    public static JdbcTemplate getJdbcTemplate(String name) {
        return jdbcTemplateMap.get(name);
    }

    public static void close() {
        jdbcTemplateMap.forEach((k, v) -> {
            if (v != null) {
                v.close();
            }
        });
    }
}
