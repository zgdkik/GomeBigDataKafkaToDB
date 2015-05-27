package com.gome.bigdata.dao;


import java.sql.Connection;


import com.gome.bigdata.utils.PropertiesUtil;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.log4j.Logger;

public class C3P0Factory {
    private static Logger log = Logger.getLogger(C3P0Factory.class.getName());

    private static ComboPooledDataSource cpds = null;

    private C3P0Factory() throws Exception {
        System.out.println("aaaaaaaaaaaaaaaaaaaaa");
        getOracleComboPooledDataSource();

    }

    public static ComboPooledDataSource getOracleComboPooledDataSource() throws Exception {
        if (cpds == null) {

            cpds = new ComboPooledDataSource();
            String DRIVER_NAME = PropertiesUtil.getInstance().getProperty("odbc.driverClass");
            String DATABASE_URL = PropertiesUtil.getInstance().getProperty("odbc.url");
            String DATABASE_USER = PropertiesUtil.getInstance().getProperty("odbc.username");
            String DATABASE_PASSWORD = PropertiesUtil.getInstance().getProperty("odbc.password");
            boolean Validate = Boolean.parseBoolean(PropertiesUtil.getInstance().getProperty("c3p0.validate"));
            int Min_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.minPoolSize"));
            int Acquire_Increment = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.acquireIncrement"));
            int Max_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.maxPoolSize"));
            int Initial_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.initialPoolSize"));
            int Idle_Test_Period = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.idleConnectionTestPeriod"));
            cpds.setDriverClass(DRIVER_NAME);
            cpds.setJdbcUrl(DATABASE_URL);
            // cpds.setJdbcUrl("jdbc:mysql://10.58.47.155:3306/data_helper?useUnicode=true&characterEncoding=utf8");
            cpds.setUser(DATABASE_USER);
            cpds.setPassword(DATABASE_PASSWORD);
            cpds.setInitialPoolSize(Initial_PoolSize);
            cpds.setMinPoolSize(Min_PoolSize);
            cpds.setMaxPoolSize(Max_PoolSize);
            cpds.setAcquireIncrement(Acquire_Increment);
            cpds.setIdleConnectionTestPeriod(Idle_Test_Period);
            cpds.setTestConnectionOnCheckout((Validate));

        }
        return cpds;
    }

    public ComboPooledDataSource getMysqlComboPooledDataSource() throws Exception {
        if (cpds == null) {

            cpds = new ComboPooledDataSource();
            String DRIVER_NAME = PropertiesUtil.getInstance().getProperty("jdbc.driverClass");
            String DATABASE_URL = PropertiesUtil.getInstance().getProperty("jdbc.url");
            String DATABASE_USER = PropertiesUtil.getInstance().getProperty("jdbc.username");
            String DATABASE_PASSWORD = PropertiesUtil.getInstance().getProperty("jdbc.password");
            boolean Validate = Boolean.parseBoolean(PropertiesUtil.getInstance().getProperty("c3p0.validate"));
            int Min_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.minPoolSize"));
            int Acquire_Increment = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.acquireIncrement"));
            int Max_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.maxPoolSize"));
            int Initial_PoolSize = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.initialPoolSize"));
            int Idle_Test_Period = Integer.parseInt(PropertiesUtil.getInstance().getProperty("c3p0.idleConnectionTestPeriod"));
            cpds.setDriverClass(DRIVER_NAME);
            cpds.setJdbcUrl(DATABASE_URL);
            // cpds.setJdbcUrl("jdbc:mysql://10.58.47.155:3306/data_helper?useUnicode=true&characterEncoding=utf8");
            cpds.setUser(DATABASE_USER);
            cpds.setPassword(DATABASE_PASSWORD);
            cpds.setInitialPoolSize(Initial_PoolSize);
            cpds.setMinPoolSize(Min_PoolSize);
            cpds.setMaxPoolSize(Max_PoolSize);
            cpds.setAcquireIncrement(Acquire_Increment);
            cpds.setIdleConnectionTestPeriod(Idle_Test_Period);
            cpds.setTestConnectionOnCheckout((Validate));

        }
        return cpds;
    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            log.info("qqqqqqqqqqqqqqqq");
            if (cpds == null) {
                log.info("wwwwwwwwwwwwwwww");
                getOracleComboPooledDataSource();
                log.info("eeeeeeeeeeeeeeeeeeeee");
            }
            log.info("rrrrrrrrrrrrrrrrrrrrrr");
            connection = cpds.getConnection();
            log.info("ttttttttttttttttttttt");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return connection;
    }

    public static void main(String[] args) throws Exception {
//		GetConfig.init("D:/tmp/config.properties");
//		Connection conn = new C3P0Factory("mysql").getConnection();
//		conn.setAutoCommit(false);
//		String sql = "INSERT INTO TEST(id,name) VALUES ('8','dfs')";
//		Statement stmt = conn.createStatement();
//		stmt.executeUpdate(sql);
//		conn.commit();
//		conn.close();
//        GetConfig.init("D:\\WorkSpace\\Gome\\gome_db_kafkatomysql\\src\\config\\config.properties");
//        Connection conn = new C3P0Factory("mysql").getConnection();
//        conn.setAutoCommit(false);
//        PreparedStatement pst = (PreparedStatement) conn.prepareStatement("insert into test values (?,'中国')");
//        for (Integer i = 0; i < 10000; i++) {
//            System.out.println(i.toString());
//            pst.setString(1, i.toString());
//            // 把一个SQL命令加入命令列表
//            pst.addBatch();
//        }
//        // 执行批量更新
//        pst.executeBatch();
//        // 语句执行完毕，提交本事务
//        conn.commit();
//        conn.close();

    }

}
