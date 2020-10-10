package com.quickstart.service;

import com.quickstart.model.ColumnMeta;
import com.quickstart.model.JdbcConfig;
import com.quickstart.model.TableMeta;
import org.junit.Test;

import java.sql.SQLException;

public class SimpleHiveTestDataCreatorTest {

    private SimpleHiveTestDataCreator creator;

    {
        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setName("hive");
        jdbcConfig.setUser("cpp");
        jdbcConfig.setPasswd("cpp");
        jdbcConfig.setDriver("org.apache.hive.jdbc.HiveDriver");
        jdbcConfig.setUrl("jdbc:hive2://bxzj-test-swift0.bxzj.baixinlocal.com:2181,bxzj-test-swift1.bxzj.baixinlocal.com:2181,bxzj-test-swift2.bxzj.baixinlocal.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        creator = new SimpleHiveTestDataCreator(jdbcConfig);
    }

    @Test
    public void createTab() throws SQLException {
        String schema = "cpp_test";
        TableMeta tabMeta = new TableMeta();
        ColumnMeta partition = new ColumnMeta();
        partition.setName("etl_dt");
        partition.setType("string");
        tabMeta.setName("cid_user_label");
        tabMeta.setPartitionCol(partition);

        ColumnMeta customerId = new ColumnMeta();
        customerId.setName("customer_id");
        customerId.setType("string");
        customerId.setRemark("customer_id");

        ColumnMeta name = new ColumnMeta();
        name.setName("name");
        name.setType("string");
        name.setRemark("name");

        ColumnMeta age = new ColumnMeta();
        age.setName("age");
        age.setType("int");
        age.setRemark("age");

        ColumnMeta loginCt = new ColumnMeta();
        loginCt.setName("login_ct");
        loginCt.setType("bigint");
        loginCt.setRemark("loginCt");

        tabMeta.addColMeta(customerId);
        tabMeta.addColMeta(name);
        tabMeta.addColMeta(age);
        tabMeta.addColMeta(loginCt);

        creator.createDBIfNotExist(schema);
        creator.createTab(schema, tabMeta);
    }
}
