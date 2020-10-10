package com.quickstart.main;

import com.google.common.collect.Lists;
import com.quickstart.dao.JdbcTemplateFactory;
import com.quickstart.model.JdbcConfig;
import com.quickstart.service.SimpleHiveTestDataCreator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;

@Slf4j
public class SimpleHiveTestDataCreatorJob {

    public static void main(String[] args) throws SQLException {
        String schema = null;
        String tabName = null;
        int batch = 10;
        int num = 1;
        String pkCol = null;
        String unCludCol = null;
        String partition = null;
        String etlDT = null;
        boolean reCreatBaseTab = false;
        String hiveUser = null;
        String hiveUserPwd = null;
        String hiveDriver = null;
        String hiveUrl = null;
        if (args != null) {
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("schema=")) {
                    schema = arg.substring(7);
                } else if (arg.startsWith("tabName=")) {
                    tabName = arg.substring(8);
                } else if (arg.startsWith("batch=")) {
                    batch = Integer.parseInt(arg.substring(6));
                } else if (arg.startsWith("num=")) {
                    num = Integer.parseInt(arg.substring(4));
                } else if (arg.startsWith("pkCol=")) {
                    pkCol = arg.substring(6);
                } else if (arg.startsWith("unCludCol=")) {
                    unCludCol = arg.substring(10);
                } else if (arg.startsWith("partition=")) {
                    partition = arg.substring(10);
                } else if (arg.startsWith("etlDT=")) {
                    etlDT = arg.substring(6);
                } else if (arg.startsWith("reCreatBaseTab=")) {
                    reCreatBaseTab = Boolean.parseBoolean(arg.substring(15));
                } else if (arg.startsWith("hiveUser=")) {
                    hiveUser = arg.substring(9);
                } else if (arg.startsWith("hiveUserPwd=")) {
                    hiveUserPwd = arg.substring(12);
                } else if (arg.startsWith("hiveDriver=")) {
                    hiveDriver = arg.substring(11);
                } else if (arg.startsWith("hiveUrl=")) {
                    hiveUrl = arg.substring(8);
                }
            }
        }
        log.info("{}={}", "schema", schema);
        log.info("{}={}", "tabName", tabName);
        log.info("{}={}", "batch", batch);
        log.info("{}={}", "num", num);
        log.info("{}={}", "pkCol", pkCol);
        log.info("{}={}", "unCludCol", unCludCol);
        log.info("{}={}", "partition", partition);
        log.info("{}={}", "etlDT", etlDT);
        log.info("{}={}", "reCreatBaseTab", reCreatBaseTab);
        log.info("{}={}", "hiveUser", hiveUser);
        log.info("{}={}", "hiveUserPwd", hiveUserPwd);
        log.info("{}={}", "hiveDriver", hiveDriver);
        log.info("{}={}", "hiveUrl", hiveUrl);

        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setName("hive");
        jdbcConfig.setUser(hiveUser);
        jdbcConfig.setPasswd(hiveUserPwd);
        jdbcConfig.setDriver(hiveDriver);
        jdbcConfig.setUrl(hiveUrl);

        SimpleHiveTestDataCreator simpleHiveTestDataCreator = new SimpleHiveTestDataCreator(jdbcConfig);
        simpleHiveTestDataCreator.createTestData(schema, tabName, pkCol, num, batch, partition, etlDT, reCreatBaseTab, Lists.newArrayList(StringUtils.split(unCludCol, ",")));
        JdbcTemplateFactory.close();
    }
}
