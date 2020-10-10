package com.quickstart.service;

import com.quickstart.dao.JdbcTemplate;
import com.quickstart.dao.JdbcTemplateFactory;
import com.quickstart.model.ColumnMeta;
import com.quickstart.model.JdbcConfig;
import com.quickstart.model.TableMeta;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SimpleHiveTestDataCreator {
    private JdbcConfig jdbcConfig;
    private JdbcTemplate jdbcTemplate;


    public SimpleHiveTestDataCreator(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        JdbcTemplateFactory.setConfigs(jdbcConfig);
        jdbcTemplate = JdbcTemplateFactory.getJdbcTemplate(jdbcConfig.getName());
    }

    public void createDBIfNotExist(String schema) throws SQLException {
        jdbcTemplate.createDBIfNotExist(schema);
    }

    public void dropTable(String schema, String tableName) throws SQLException {
        String sql = "drop table " + schema + "." + tableName;
        log.info(sql);
        jdbcTemplate.execute(sql);
    }

    public void createTab(String schema, TableMeta tableMeta) throws SQLException {
        List<String> cols = tableMeta.getColumnMetaList().stream().map(e -> String.format(" %s %s comment '%s'", e.getName(), e.getType(), e.getRemark())).collect(Collectors.toList());

        ColumnMeta partitionCol = tableMeta.getPartitionCol();
        String createSql = String.format("create table %s.%s(%s) partitioned by ( %s %s ) stored as parquet tblproperties ('parquet.compression'='snappy') ",
                schema, tableMeta.getName(),
                String.join(",", cols),
                partitionCol.getName(), partitionCol.getType()
        );
        log.info("createSql={}", createSql);
        jdbcTemplate.execute(createSql);
    }

    public void createTestData(String schema, String tabName, String pkCol, int num, int batch, String partition, String etlDT, boolean reCreatBaseTab, List<String> unCludColList) throws SQLException {

        // 检测hive表是否存在
        // 加载表结构
        TableMeta tabMeta = jdbcTemplate.getTabMeta(schema, tabName);
        if (tabMeta.getName() == null) {
            log.error("can not found tab:{}", tabName);
            return;
        }
        TableMeta baseTestTabMeta = jdbcTemplate.getTabMeta(schema, "base_test");
        if (baseTestTabMeta.getName() == null) {
            reCreatBaseTab = true;
        }
        if (reCreatBaseTab) {
            createBaseTestTab(schema);
        }
        createTestData(schema, tabName, pkCol, num, batch, partition, etlDT, unCludColList);
        // 检测表数据量
        String countSql = String.format("select count(1) from %s.%s where etl_dt = '%s'", schema, tabName, etlDT);
        long count = jdbcTemplate.count(countSql);
        log.info("count:{}", count);
        jdbcTemplate.close();

    }

    public void createBaseTestTab(String schema) throws SQLException {
        log.info("start recreatBaseTab:{}.base_test", schema);
        //加载基表
        List<String> baseTabDatas = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            baseTabDatas.add(String.format(" select 1 as col_1 "));
        }
        jdbcTemplate.execute(String.format("drop table %s.base_test", schema));
        String baseTabCreatSql = String.format("create table %s.base_test as select col_1 from (%s) temp", schema, String.join("union all", baseTabDatas));
        jdbcTemplate.execute(baseTabCreatSql);
    }

    public void createTestData(String schema, String tabName, String pkCol, int num, int batch, String partition, String etlDT, List<String> unCludColList) throws SQLException {
        List<ColumnMeta> cols = jdbcTemplate.getAllCols(schema, tabName);
        // 拼接制造数据的sql
        String overwriteSqlTemplat = "insert overwrite table %s.%s partition(%s = '%s') select %s from (select %s from %s) T group by %s limit %s";
        List<String> randomColVals = new ArrayList<>(cols.size());
        List<String> selCols = new ArrayList<>(cols.size());

        for (ColumnMeta cm : cols) {
            if (unCludColList.contains(cm.getName())) {
                continue;
            }
            if (cm.getType().equalsIgnoreCase("string")) {
                randomColVals.add(String.format("concat('" + cm.getName() + "_',ceiling(rand()*%s)) as %s", cm.getName().equalsIgnoreCase(pkCol) ? Integer.MAX_VALUE : 100, cm.getName()));
            } else {
                randomColVals.add(String.format("ceiling(rand()*%s) as %s", Integer.MAX_VALUE, cm.getName()));
            }
            selCols.add("max(" + cm.getName() + ")");
        }

        List<String> baseTabs = new ArrayList<>(num);
        for (int i = 0; i < num; i++) {
            String subSql = "";
            if (i > 0) {
                subSql = " join ";
            }
            //增大数据量，以防重复数据导致最后的结果条数不对
            int size = (int) (batch * 1.5);
            subSql = subSql + String.format("(select '' from %s.base_test limit %s) base_test_%s", schema, size, i);
            if (i > 0) {
                subSql += " on 1=1 ";
            }
            baseTabs.add(subSql);
        }
        String overwriteSql = String.format(overwriteSqlTemplat, schema, tabName, partition, etlDT,
                String.join(",", selCols),
                String.join(",", randomColVals),
                String.join("", baseTabs),
                pkCol, num * batch);
        log.info("overwriteSql={}", overwriteSql);
        // 执行sql
        jdbcTemplate.execute(overwriteSql);
    }


}
