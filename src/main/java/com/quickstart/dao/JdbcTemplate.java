package com.quickstart.dao;

import com.quickstart.model.ColumnMeta;
import com.quickstart.model.JdbcConfig;
import com.quickstart.model.TableMeta;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.BasicRowProcessor;
import org.apache.commons.dbutils.BeanProcessor;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class JdbcTemplate {
    private static String SQL_LIMIT = "select * from ( %s ) GTAB limit %d";
    private static String SELECT_TEMPLAT = "SELECT * FROM %s limit 1";
    private JdbcConfig jdbcConfig;
    private Connection conn;

    public JdbcTemplate(JdbcConfig jdbcConfig) {
        this.jdbcConfig = jdbcConfig;
        try {
            Class.forName(jdbcConfig.getDriver());
            conn = getConn();
        } catch (Exception e) {
            log.error("", e);
            e.printStackTrace();
            System.exit(-1);
        }
    }


    private synchronized Connection getConn() throws SQLException {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(this.jdbcConfig.getUrl(), this.jdbcConfig.getUser(), this.jdbcConfig.getPasswd());
            } catch (SQLException e) {
                throw new SQLException("Connect to MySql Server Error : " + e.getMessage());
            }
        }
        return conn;
    }


    /**
     * wh:执行非查询类SQL
     *
     * @param sql
     */
    public void execute(String sql) throws SQLException {
        Connection conn = getConn();
        Statement stat = conn.createStatement();
        stat.execute(sql);
    }


    /**
     * wh: 查询类sql执行
     *
     * @param sql
     * @throws SQLException
     */
    public void query4print(String sql) throws SQLException {
        sql = String.format(SQL_LIMIT, sql, this.jdbcConfig.getShowCount());
        System.out.println(sql);
        Connection conn = getConn();
        Statement stat = conn.createStatement();
        stat.setFetchSize(200);
        ResultSet rs = stat.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            System.out.print(metaData.getColumnName(i) + "\t|");
        }
        while (rs.next()) {
            System.out.println();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.print(rs.getObject(i) + "\t|");
            }
        }
        System.out.println();

    }

    public void printTableList() throws SQLException {
        for (TableMeta meta : getAllTab()) {
            System.out.println(meta);
        }
    }


    public List<TableMeta> getAllTab() throws SQLException {
        //获取数据库的元数据
        DatabaseMetaData dbMetaData = conn.getMetaData();
        //从元数据中获取到所有的表名
        ResultSet rs = dbMetaData.getTables(null, null, null, new String[]{"TABLE"});
        List<TableMeta> tableMetas = new ArrayList<TableMeta>();
        while (rs.next()) {
            TableMeta meta = new TableMeta();
            meta.setName(rs.getString("TABLE_NAME"));
            meta.setType(rs.getString("TABLE_TYPE"));
            meta.setCat(rs.getString("TABLE_CAT"));
            meta.setUserName(rs.getString("TABLE_SCHEM"));
            meta.setRemark(rs.getString("REMARKS"));
            tableMetas.add(meta);
        }
        return tableMetas;
    }

    /**
     * 获取表中所有字段名称
     *
     * @param tableName 表名
     * @return
     */
    private List<String> getAllColNames(String tableName) {
        List<String> columnNames = new ArrayList<>();
        try (PreparedStatement pStemt = conn.prepareStatement(String.format(SELECT_TEMPLAT, tableName))) {
            // 结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            // 表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnNames;
    }

    public List<ColumnMeta> getAllCols(String schema, String tabName) {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        try {
            //获取数据库的元数据
            DatabaseMetaData dbMetaData = conn.getMetaData();
            //从元数据中获取到所有的表名
            ResultSet colRet = dbMetaData.getColumns(null, schema, tabName, "%");
            while (colRet.next()) {
                ColumnMeta columnMeta = new ColumnMeta();
                columnMeta.setName(colRet.getString("COLUMN_NAME"));
                columnMeta.setType(colRet.getString("TYPE_NAME"));
                columnMeta.setSize(colRet.getInt("COLUMN_SIZE"));
                columnMeta.setDigits(colRet.getInt("DECIMAL_DIGITS"));
                int nullable = colRet.getInt("NULLABLE");
                columnMeta.setNullAble(nullable == 1 ? true : false);
                columnMetas.add(columnMeta);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return columnMetas;
    }

    public TableMeta getTabMeta(String schema, String tabName) throws SQLException {
        //获取数据库的元数据
        DatabaseMetaData dbMetaData = conn.getMetaData();
        //从元数据中获取到所有的表名
        ResultSet rs = dbMetaData.getTables(null, schema, tabName, new String[]{"TABLE"});
        while (rs.next()) {
            TableMeta meta = new TableMeta();
            meta.setName(rs.getString("TABLE_NAME"));
            meta.setType(rs.getString("TABLE_TYPE"));
            meta.setCat(rs.getString("TABLE_CAT"));
            meta.setUserName(rs.getString("TABLE_SCHEM"));
            meta.setRemark(rs.getString("REMARKS"));
            return meta;
        }
        return new TableMeta();
    }

    /**
     * <p>
     * 生成建表语句
     * </p>
     *
     * @param tableName
     * @return
     */
    public String getCreateTabSql(String tableName) {
        try (PreparedStatement pstmt = conn.prepareStatement(String.format("SHOW CREATE TABLE %s", tableName))) {
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String ddl = rs.getString(2);
                Pattern compile = Pattern.compile("AUTO_INCREMENT=\\d+\\s");
                Matcher matcher = compile.matcher(ddl);
                String sql = matcher.replaceFirst(" AUTO_INCREMENT=1 ") + ";\n";
                sql = sql.replaceAll("timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'", "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
                sql = sql.replaceAll("timestamp NOT NULL COMMENT", "timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT");
                sql = sql.replaceAll("DEFAULT '0000-00-00 00:00:00.000'", "DEFAULT '1980-01-01 00:00:00'");
                sql = sql.replaceAll("DEFAULT '1980-01-01 00:00:00.000'", "DEFAULT '1980-01-01 00:00:00'");
                return sql;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public synchronized void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
            } finally {
                conn = null;
            }
        }
    }

    public List<Map<String, Object>> list(String sql) {
        try {
            Connection conn = getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();
            ArrayList<Map<String, Object>> list = new ArrayList<>();
            while (rs.next()) {
                HashMap map = new HashMap();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    map.putIfAbsent(metaData.getColumnName(i), rs.getObject(i));
                }
                list.add(map);
            }
            rs.close();
            return list;
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return new ArrayList<>(0);
    }

    public <T> List<T> list(String sql, Class<T> clazz, Map map) {
        try {
            QueryRunner qRunner = new QueryRunner();
            List<T> query = (List<T>) qRunner.query(
                    conn,
                    sql,
                    new BeanListHandler(
                            clazz, new BasicRowProcessor(new BeanProcessor(map))));
            if (!query.isEmpty()) {
                return query;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return new ArrayList<>(0);
    }

    public <T> T getOne(String sql, Class<T> clazz, Map map) {
        try {

            QueryRunner qRunner = new QueryRunner();
            T query = (T) qRunner.query(
                    this.conn,
                    sql,
                    new BeanHandler(
                            clazz, new BasicRowProcessor(new BeanProcessor(map))));
            if (query != null) {
                return query;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return null;
    }

    public long count(String sql) {
        try {
            Connection conn = getConn();
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            rs.next();
            return rs.getLong(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("", e);
        }
        return -1;
    }


    public void createDBIfNotExist(String schema) throws SQLException {
        ResultSet rs = getConn().getMetaData().getCatalogs();
        boolean exist = false;
        while (rs.next()) {
            if (schema.equalsIgnoreCase(rs.getString("TABLE_CAT"))) {
                exist = true;
                break;
            }
        }
        if (!exist) {
            String createSql = "create database " + schema;
            log.info(createSql);
            execute(createSql);
        }
    }
}
