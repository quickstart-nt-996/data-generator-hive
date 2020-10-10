package com.quickstart.main;

import org.junit.Test;

import java.sql.SQLException;

public class DataTestCreatorTest {

    @Test
    public void main() throws SQLException {
        SimpleHiveTestDataCreatorJob.main(new String[]{
                "schema=cpp_c",
                "tabName=bdp_cid_user_label",
                "batch=10",
                "num=3",
                "pkCol=customer_id",
                "unCludCol=etl_dt",
                "partition=etl_dt",
                "etlDT=20200929",
                "reCreatBaseTab=true",
                "hiveUser=cpp",
                "hiveUserPwd=cpp",
                "hiveDriver=org.apache.hive.jdbc.HiveDriver",
                "hiveUrl=jdbc:hive2://bxzj-test-swift0.bxzj.baixinlocal.com:2181,bxzj-test-swift1.bxzj.baixinlocal.com:2181,bxzj-test-swift2.bxzj.baixinlocal.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"
        });
    }
}
