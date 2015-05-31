package com.gome.bigdata.main;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.ConfAttr;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.bigdata.monitor.OracleSqlBufferMonitor;
import com.gome.bigdata.monitor.SaveHourCountTimer;
import com.gome.bigdata.process.KafkaConsumer;
import com.gome.bigdata.process.SaveToOracleExecutor;
import com.gome.bigdata.utils.ParseUtil;
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

    public static void resetReceivedFromKafkaOptCount() {
        receivedFromKafkaOptCount.set(0);
    }

    public static void resetSaveToOracleSuccessCount() {
        saveToOracleSuccessCount.set(0);
    }

    public static void resetSaveToOracleFailureCount() {
        saveToOracleFailureCount.set(0);
    }

    public void initConfig() {
        log.info("--------------------Start common init configuration----------------------");
        ConfAttr.BQ_BUFFER_SIZE = Integer.parseInt(PropertiesUtil.getInstance().getProperty("blockingqueue_size"));
        ConfAttr.BUFFER_MONITOR_FILE = PropertiesUtil.getInstance().getProperty("buffer_monitor_file");
        ConfAttr.HOUR_RESET_COUNT_FILE = PropertiesUtil.getInstance().getProperty("count_hour_monitor_file");
        ConfAttr.MEM_MONITOR_SECONDS = Integer.parseInt(PropertiesUtil.getInstance().getProperty("mem_monitor_seconds"));

        log.info("-----------------Start Oracle init configuration------------------------");
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
        OracleAttr.TO_ORACLE_THREAD_NUM = Integer.parseInt(PropertiesUtil.getInstance().getProperty("toOracle_thread"));
        OracleAttr.ERROR_SQL_PATH = PropertiesUtil.getInstance().getProperty("error_sql_log");

    }

    /**
     * 开始写入Oracle的进程及相关监控
     */
    public void startOracleEntry() {
        this.oracleSqlBuffer = new LinkedBlockingDeque<JSONObject>(ConfAttr.BQ_BUFFER_SIZE);
        this.startHourCountMonitor();
        this.startBufferMonitor(ConfAttr.MEM_MONITOR_SECONDS);
        this.startSaveToOracleExecutor(OracleAttr.TO_ORACLE_THREAD_NUM);
    }

    /**
     * 启动内存监控现场
     *
     * @param seconds 多长时间打印一次内存监控，单位 s
     */
    private void startBufferMonitor(int seconds) {
        this.bufferMonitor = new OracleSqlBufferMonitor(oracleSqlBuffer);
        this.bufferMonitor.start(5, seconds);
    }

    /**
     * 启动每小时进行数据统计的 进程
     */
    private void startHourCountMonitor() {
        long delay = ParseUtil.getFirstClockLong() - System.currentTimeMillis();
        SaveHourCountTimer saveCount = new SaveHourCountTimer();
        saveCount.start(delay, 3600);
    }

    /**
     * 启动从BQ到Oracle的进程
     *
     * @param n 启动的进程数，1个就可以
     */
    private void startSaveToOracleExecutor(int n) {
        for (int i = 0; i < n; i++) {
            log.info("--------Start SaveToOracleExecutor-------");
            SaveToOracleExecutor oracleExecutor = new SaveToOracleExecutor(this.oracleSqlBuffer);
            Thread t = new Thread(oracleExecutor);
            t.start();
            consumers.add(oracleExecutor);
        }
    }

    /**
     * 启动 kafka Consumer
     *
     * @param offsetReset kafka offset
     * @param zkQuorum    zookeeper地址
     * @param group       消费groupid
     * @param topic       消费的topic
     * @param numThread   启动的进程数，1就行
     */
    public void startKafkaConsumer(String offsetReset, String zkQuorum, String group, String topic, int numThread) {
        this.kafkaConsumer = new KafkaConsumer(this.oracleSqlBuffer);
        log.info("-------------------Kafka consumer starting-------------------");
        kafkaConsumer.consume(offsetReset, zkQuorum, group, topic, numThread);

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
        log.info("------Oracle Buffer is empty-------------");
        for (SaveToOracleExecutor executor : consumers) {
            executor.stop();
        }
//        kafkaConsumer.complete =true ;

    }
}
