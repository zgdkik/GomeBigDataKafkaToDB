package com.gome.bigdata.main;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.process.KafkaConsumer;
import com.gome.bigdata.process.SaveToOracleExecutor;
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
        oracleSqlBuffer = new LinkedBlockingDeque<JSONObject>(10000);
    }

    public void startKafkaConsumer(String offsetReset, String zkQuorum, String group, String topic, int numThread) {
        this.kafkaConsumer = new KafkaConsumer(this.oracleSqlBuffer);
        log.info("-------------------Kafka consumer starting-------------------");
        kafkaConsumer.consume(offsetReset, zkQuorum, group, topic, numThread);

    }

    public void startSaveToOracleExecutor(int n){
        for (int i = 0; i < n; i++) {
            log.info("--------Start SaveToOracleExecutor-------");
            SaveToOracleExecutor oracleExecutor = new SaveToOracleExecutor(this.oracleSqlBuffer);
            Thread t = new Thread(oracleExecutor);
            t.start();
            consumers.add(oracleExecutor);
        }
    }


}
