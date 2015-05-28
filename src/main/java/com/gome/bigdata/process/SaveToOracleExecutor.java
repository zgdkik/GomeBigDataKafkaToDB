package com.gome.bigdata.process;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.bigdata.dao.C3P0Factory;
import com.gome.bigdata.dao.C3P0Utils;
import com.gome.bigdata.main.OracleEntry;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by lujia on 2015/5/25.
 */
public class SaveToOracleExecutor implements Runnable {

    private static Logger log = Logger.getLogger(SaveToOracleExecutor.class);

    private final BlockingQueue<JSONObject> queue;
    private ComboPooledDataSource dataSource;
    private AtomicBoolean run = new AtomicBoolean(true);

    public SaveToOracleExecutor(BlockingQueue<JSONObject> queue) {
        this.queue = queue;
        try {
            dataSource = C3P0Factory.getOracleComboPooledDataSource();
        } catch (Exception e) {
            log.error("Data Source initial error: " + e.getMessage());
            e.printStackTrace();
            System.exit(0);
        }
    }


    List<String> preSqlList = new ArrayList<String>();

    @Override
    public void run() {
        Connection conn = null;
        String sql = "";
        try {
            conn = C3P0Factory.getConnection();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            while (run.get()) {

                if (conn == null) {
                    conn = C3P0Factory.getConnection();
                    conn.setAutoCommit(false);
                    stmt = conn.createStatement();
                }

                try {
                    sql = queue.take().getString("sql");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    log.error("Taking out sql error: " + e.getMessage());
                }
                try {
                    stmt.execute(sql);
                } catch (SQLException e) {
                    log.error("Oracle ERROR! ERROR SQL: " + sql);
                    remedyCommit();
                    continue;
                }

                preSqlList.add(sql);
                if (preSqlList.size() >= OracleAttr.ORACLE_BATCH_NUM) {
                    try {
                        conn.commit();
                        OracleEntry.incrSaveToOracleSuccessCount(preSqlList.size());
                        preSqlList.clear();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        log.error("Batch submit error!");
                        remedyCommit();
                    }
                }
            }
        } catch (SQLException e) {
            log.error("EXECUTE SQL ERROR: " + e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                    log.info("main connection closed...");
                } catch (SQLException e) {
                    log.error("Finally close connection!");
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 停止程序时调用
     */
    public void stop() {
        run.set(false);
        while (preSqlList.size() > 0) {
            log.info("-----Batch sql list-----" + preSqlList.size());
            for (int i = 0; i < preSqlList.size(); i++) {
                log.info("single sql: " + i + " - " + preSqlList.get(i));
                singleCommit(preSqlList.get(i));
            }
        }
        while (this.queue.size() > 0) {
            try {
                String sql = this.queue.take().getString("sql");
                singleCommit(sql);
            } catch (InterruptedException e) {
                log.error("Get sql from quere error! " + this.queue.size() + "\n" + e.getMessage());
            }
        }
    }

    /**
     * 提交单个sql
     *
     * @param sql
     */
    private void singleCommit(String sql) {
        try {
            log.info("1111111111111111111111");
            Connection conn = C3P0Factory.getConnection();
            log.info("3333333333333333333333333333");
            conn.setAutoCommit(false);
            log.info("4444444444444444444444");
            Statement stmt = conn.createStatement();
            log.info("55555555555555555555555555555");
            log.info(stmt.getClass());
            stmt.execute(sql);
            log.info("6666666666666666666666666666");
            conn.commit();
            log.info("777777777777777777777777");
            conn.close();
            log.info("88888888888888888888888888888888");
            OracleEntry.incrSaveToOracleSuccessCount(1);
            log.info("222222222222222222222");
        } catch (SQLException e) {
            log.info("ooooooooooooooooooooooooooooo");
            OracleEntry.incrSaveToOracleFailureCount(1);
            log.error("EXECUTE ERROR SQL: " + sql + "\n" + e.getMessage());
        }
    }

    /**
     * 批量提交sql时 出现异常就调用
     */
    private void remedyCommit() {
        for (int i = 0; i < preSqlList.size(); i++) {
            singleCommit(preSqlList.get(i));
        }
        preSqlList.clear();
    }
}
