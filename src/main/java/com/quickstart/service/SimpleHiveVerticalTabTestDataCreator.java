package com.quickstart.service;

import com.quickstart.dao.JdbcTemplate;
import com.quickstart.dao.JdbcTemplateFactory;
import com.quickstart.model.ColumnMeta;
import com.quickstart.model.JdbcConfig;
import com.quickstart.model.TableMeta;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@NoArgsConstructor
public class SimpleHiveVerticalTabTestDataCreator {
    private JdbcConfig jdbcConfig;
    @Setter
    private JdbcTemplate jdbcTemplate;


    public SimpleHiveVerticalTabTestDataCreator(JdbcConfig jdbcConfig) {
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
        ColumnMeta partitionCol = tableMeta.getPartitionCol();
        ColumnMeta pkCol = tableMeta.getPkCol();
        String createSql = String.format("create table %s.%s(%s) partitioned by ( etl_dt string,%s %s ) stored as parquet tblproperties ('parquet.compression'='snappy') ",
                schema, tableMeta.getName(),
                String.format("%s %s,val string", pkCol.getName(), pkCol.getType()),
                partitionCol.getName(), partitionCol.getType()
        );
        log.info("createSql={}", createSql);
        jdbcTemplate.execute(createSql);
    }

    public void createTestData(String schema, String tabName, String pkCol, int num, int batch, ColumnMeta partition, String etlDT, boolean reCreatBaseTab, List<String> unCludColList) throws SQLException {

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
        createTestData(schema, tabMeta, pkCol, num, batch, etlDT, unCludColList);
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
        for (int i = 0; i < 10; i++) {
            baseTabDatas.add(String.format(" select 1 as col_1 "));
        }
        jdbcTemplate.execute(String.format("drop table %s.base_test", schema));
        String baseTabCreatSql = String.format("create table %s.base_test as select col_1 from (%s) temp", schema, String.join("union all", baseTabDatas));
        jdbcTemplate.execute(baseTabCreatSql);
    }

    public void createTestData(String schema, TableMeta tableMeta, String pkCol, int num, int batch, String etlDT, List<String> unCludColList) throws SQLException {
        List<ColumnMeta> cols = tableMeta.getColumnMetaList();
        // 拼接制造数据的sql
        String overwriteSqlTemplat = "insert overwrite table %s.%s partition(etl_dt = '%s',%s = '%s') select %s from %s";
        for (ColumnMeta cm : cols) {
            if (unCludColList.contains(cm.getName())) {
                continue;
            }
            String colVal;
            if (cm.getType().equalsIgnoreCase("string")) {
                colVal = String.format("ceiling(rand()*%s),concat('" + cm.getName() + "_',ceiling(rand()*%s))", Integer.MAX_VALUE, cm.getName().equalsIgnoreCase(pkCol) ? Integer.MAX_VALUE : 100);
            } else {
                colVal = String.format("ceiling(rand()*%s),ceiling(rand()*%s)", Integer.MAX_VALUE, Integer.MAX_VALUE);
            }
            List<String> baseTabs = new ArrayList<>(num);
            for (int i = 0; i < num; i++) {
                String subSql = "";
                if (i > 0) {
                    subSql = " join ";
                }
                subSql = subSql + String.format("(select '' from %s.base_test limit %s) base_test_%s", schema, batch, i);
                if (i > 0) {
                    subSql += " on 1=1 ";
                }
                baseTabs.add(subSql);
            }
            String overwriteSql = String.format(overwriteSqlTemplat, schema, tableMeta.getName(), etlDT, cm.getPartitionName(), cm.getName(), colVal, String.join("", baseTabs));
            log.info("overwriteSql={}", overwriteSql);
            jdbcTemplate.execute(overwriteSql);
        }
    }
}
