package com.gome.bigdata.dao;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.gome.bigdata.utils.PropertiesUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.log4j.Logger;

public class ConnectionFactory {
    private static Logger log = Logger.getLogger(ConnectionFactory.class.getName());

    private ConnectionFactory() {
    }

    private static ComboPooledDataSource ds = null;

    static {
        try {
            // Logger log = Logger.getLogger("com.mchange"); // 日志
            // log.setLevel(Level.WARNING);
            ds = new ComboPooledDataSource();
            // 设置JDBC的Driver类
            ds.setDriverClass(PropertiesUtil.getInstance().getProperty("odbc.driverClass"));  // 参数由 Config 类根据配置文件读取
            // 设置JDBC的URL
            ds.setJdbcUrl(PropertiesUtil.getInstance().getProperty("odbc.url"));
            // 设置数据库的登录用户名
            ds.setUser(PropertiesUtil.getInstance().getProperty("odbc.username"));
            // 设置数据库的登录用户密码
            ds.setPassword(PropertiesUtil.getInstance().getProperty("odbc.password"));
            // 设置连接池的最大连接数
            ds.setMaxPoolSize(20);
            // 设置连接池的最小连接数
            ds.setMinPoolSize(20);
        } catch (PropertyVetoException e) {
            System.out.println("aaaaaaaa");
            e.printStackTrace();
            log.error("C3P0 initial error: \n" + e.getMessage());
        }
    }

    public static synchronized Connection getConnection() {
        Connection con = null;
        try {
            con = ds.getConnection();
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        return con;
    }
    // C3P0 end
}

