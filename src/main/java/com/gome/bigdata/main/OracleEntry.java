package com.gome.bigdata.main;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.ConfAttr;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.bigdata.monitor.OracleSqlBufferMonitor;
import com.gome.bigdata.process.KafkaConsumer;
import com.gome.bigdata.process.SaveToOracleExecutor;
import com.gome.bigdata.utils.PropertiesUtil;
import com.gome.bigdata.utils.StringUtil;
import org.apache.log4j.Logger;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by lujia on 2015/5/22.
 */
public class OracleEntry {

    private static Logger log = Logger.getLogger(OracleEntry.class.getName());

    //记录每一个操作的表名(table)、操作(opt)、主键的hashcode和sql
    public BlockingQueue<JSONObject> oracleSqlBuffer = null;
    private static AtomicInteger receivedFromKafkaOptCount = new AtomicInteger(0);
    private static AtomicInteger saveToOracleSuccessCount = new AtomicInteger(0);
    private static AtomicInteger saveToOracleFailureCount = new AtomicInteger(0);

    KafkaConsumer kafkaConsumer = null;
    OracleSqlBufferMonitor bufferMonitor = null;
    private Vector<SaveToOracleExecutor> consumers = new Vector<SaveToOracleExecutor>();

    public static int getReceivedFromKafkaOptCount() {
        return receivedFromKafkaOptCount.get();
    }

    public static int incrReceivedFromKafkaOptCount(int n) {
        return receivedFromKafkaOptCount.addAndGet(n);
    }

    public static int getSaveToOracleSuccessCount() {
        return saveToOracleSuccessCount.get();
    }

    public static int incrSaveToOracleSuccessCount(int n) {
        return saveToOracleSuccessCount.addAndGet(n);
    }

    public static int getSaveToOracleFailureCount() {
        return saveToOracleFailureCount.get();
    }

    public static int incrSaveToOracleFailureCount(int n) {
        return saveToOracleFailureCount.addAndGet(n);
    }

    public void initConfig() {
        log.info("--------------------Start init configuration----------------------");
        ConfAttr.BQ_BUFFER_SIZE = Integer.parseInt(PropertiesUtil.getInstance().getProperty("blockingqueue_size"));

        OracleAttr.SSO_ERROR_TABLE = PropertiesUtil.getInstance().getProperty("error_table");
        OracleAttr.SSO_ERROR_COLUMN = PropertiesUtil.getInstance().getProperty("error_column");
        OracleAttr.SSO_CORRECT_COLUMN = PropertiesUtil.getInstance().getProperty("correct_column");

        String changeOwner = PropertiesUtil.getInstance().getProperty("change_owner");
        if (changeOwner == null || StringUtil.isEmpty(changeOwner, true)) {
            OracleAttr.CHANGE_OWNER = null;
        } else {
            OracleAttr.CHANGE_OWNER = changeOwner;
        }

        OracleAttr.ORACLE_BATCH_NUM = Integer.parseInt(PropertiesUtil.getInstance().getProperty("toOracle_batch_size"));

    }

    public void intOracleEntry() {
        this.oracleSqlBuffer = new LinkedBlockingDeque<JSONObject>(ConfAttr.BQ_BUFFER_SIZE);
    }

    public void startBufferMonitor(int time) {
        this.bufferMonitor = new OracleSqlBufferMonitor(oracleSqlBuffer);
        this.bufferMonitor.start(5, time);
    }


    public void startKafkaConsumer(String offsetReset, String zkQuorum, String group, String topic, int numThread) {
        this.kafkaConsumer = new KafkaConsumer(this.oracleSqlBuffer);
        log.info("-------------------Kafka consumer starting-------------------");
        kafkaConsumer.consume(offsetReset, zkQuorum, group, topic, numThread);

    }

    public void startSaveToOracleExecutor(int n) {
        for (int i = 0; i < n; i++) {
            log.info("--------Start SaveToOracleExecutor-------");
            SaveToOracleExecutor oracleExecutor = new SaveToOracleExecutor(this.oracleSqlBuffer);
            Thread t = new Thread(oracleExecutor);
            t.start();
            consumers.add(oracleExecutor);
        }
    }


    /**
     * 程序退出时调用
     */
    public void stop() {
        log.info("--------------------Start Stopping Oracle Entry--------------------");
        this.kafkaConsumer.stop();
        log.info("------Kafka Consumer stopped-------------");
        while (this.oracleSqlBuffer.size() != 0) {
            // 等buffer中的数据全部取完
        }
        log.info("------Hbase Put Buffer is empty-------------");
        for (SaveToOracleExecutor executor : consumers) {
            executor.stop();
        }

    }
}
