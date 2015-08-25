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
    private AtomicBoolean run = new AtomicBoolean(true);

    public SaveToOracleExecutor(BlockingQueue<JSONObject> queue) {
        this.queue = queue;
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

                if (queue.remainingCapacity() <= 0) {
                    continue;

                } else {
                    try {
                        sql = queue.take().getString("sql");
                        preSqlList.add(sql);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                        log.error("Taking out sql error: " + e.getMessage());
                    }
                    try {
                        stmt.execute(sql);
                    } catch (SQLException e) {
                        log.error("Oracle ERROR! ERROR SQL: " + sql + "\n" + e.getMessage());
                        conn.close();
                        conn = null;
                        remedyCommit();
                        continue;
                    }

//                    preSqlList.add(sql);
                    if (preSqlList.size() >= OracleAttr.ORACLE_BATCH_NUM) {
                        try {
                            conn.commit();
                            OracleEntry.incrSaveToOracleSuccessCount(preSqlList.size());
                            preSqlList.clear();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            log.error("Batch submit error!");
                            conn.close();
                            conn = null;
                            remedyCommit();
                        }
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
        while (this.queue.size() > 0) {

        }
        run.set(false);

        log.info("-----Batch sql list-----" + preSqlList.size());
        for (int i = 0; i < preSqlList.size(); i++) {
            log.info("single sql: " + i + " - " + preSqlList.get(i));
            singleCommit(preSqlList.get(i));
        }
        log.info("-------------save to oracle executor stopped------------------");
    }

    /**
     * 提交单个sql
     *
     * @param sql
     */
    private void singleCommit(String sql) {
        log.info("Single commit! - " + sql);
        try {
            Connection conn = C3P0Factory.getConnection();
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();
            log.info(stmt.getClass());
            stmt.execute(sql);
//            stmt.executeUpdate(sql);
            conn.commit();
            conn.close();
            OracleEntry.incrSaveToOracleSuccessCount(1);
        } catch (SQLException e) {
            log.error("Single commit ERROR! - " + sql);
            OracleEntry.incrSaveToOracleFailureCount(1);
            log.error("EXECUTE ERROR SQL: " + sql + "\n" + e.getMessage());
        }
    }

    /**
     * 批量提交sql时 出现异常就调用
     */
    private void remedyCommit() {
        log.info("Remedy commit! preSqlList size: " + preSqlList.size());
        for (int i = 0; i < preSqlList.size(); i++) {
            singleCommit(preSqlList.get(i));
        }
        preSqlList.clear();
    }
}
