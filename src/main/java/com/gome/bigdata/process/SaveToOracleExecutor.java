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




    @Override
    public void run() {
        System.out.println("111111111111111111111111");
        int preSubmitCount = 0;
        List<String> preSqlList = new ArrayList<String>();
        try {
            System.out.println("22222222222222222222");
            Connection conn = C3P0Factory.getConnection();
            System.out.println("333333333333333333333");
            conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();

            while (run.get()) {

                if (conn == null) {
                    conn = C3P0Factory.getConnection();
                    conn.setAutoCommit(false);
                    stmt = conn.createStatement();
                }

                String sql = "";
                try {
                    sql = queue.take().getString("sql");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    log.error("Taking out sql error");
                }
                stmt.execute(sql);
                preSqlList.add(sql);
                preSubmitCount++;
                if (preSubmitCount >= OracleAttr.ORACLE_BATCH_NUM) {
                    try {
                        conn.commit();
                        OracleEntry.incrSaveToOracleSuccessCount(preSubmitCount);
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Batch submit error!");
                        //todo 调用一个一个commit preSqlList
                    } finally {
                        preSubmitCount = 0;
                        conn.close();
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Oracle ERROR!");
            e.printStackTrace();

        }

    }
}
