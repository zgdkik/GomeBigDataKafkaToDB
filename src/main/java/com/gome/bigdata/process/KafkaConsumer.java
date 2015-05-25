package com.gome.bigdata.process;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.attr.ConfAttr;
import com.gome.bigdata.attr.OracleAttr;
import com.gome.bigdata.main.OracleEntry;
import com.gome.bigdata.parse.OracleParser;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by lujia on 2015/3/20.
 */
public class KafkaConsumer {
    private static Logger log = Logger.getLogger(KafkaConsumer.class.getName());

    private final BlockingQueue<JSONObject> queue;
    private ConsumerConnector connector = null;
    private Properties props = null;

    public KafkaConsumer(BlockingQueue<JSONObject> queue) {
        this.queue = queue;
    }

    public void stop() {
        log.info("--------Kafka Consumer stop---------");
        if (connector != null) {
            connector.shutdown();
            log.info("-------Kafka Consumer Connector shutdown-------");
        }
        if (props != null) {
            props = null;
            log.info("-------Kafka Consumer Props set to null---------");
        }
    }

    public void consume(String offsetReset, String zkQuorum, String group, String topic, int numThread) {
        props = new Properties();
        props.put("zookeeper.connect", zkQuorum);
//        props.put("zookeeper.connectiontimeout.ms", "1000000");
        props.put("auto.offset.reset", "smallest");
//        props.put("auto.offset.reset", offsetReset);
//        props.put("zookeeper.sync.time.ms", "200");
//        props.put("auto.commit.interval.ms", "1000");
        props.put("group.id", group);

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        connector = Consumer.createJavaConsumerConnector(consumerConfig);

        int numThreads = numThread;
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put(topic, numThreads);

        Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = connector.createMessageStreams(topicMap);
        List<KafkaStream<byte[], byte[]>> streams = topicMessageStreams.get(topic);

        KafkaStream<byte[], byte[]> stream = streams.get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> item = it.next();
            String msg = new String(item.message());
            System.out.println("offset:" + item.offset());
//            System.out.println(Thread.currentThread().getId() + ": receive : " + msg);
            parseOpt(msg);
            try {
                Thread.sleep(1000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    private void parseOpt(String batchOpt) {
        ArrayList<JSONObject> optLists = OracleParser.parseOperations(batchOpt);
        OracleEntry.incrReceivedFromKafkaOptCount(optLists.size());


        if (this.queue.size() > (ConfAttr.BQ_BUFFER_SIZE * 0.9)) {
            try {
                Thread.sleep(2000l);
            } catch (InterruptedException e) {
                log.warn("BQ capacity has over 90%...");
            }
        }

        while (this.queue.remainingCapacity() <= optLists.size()) {
            try {
                Thread.sleep(2000l);
            } catch (InterruptedException e) {
                log.warn("BQ does not have enough room to save operations!");
            }
        }

        for (JSONObject opt : optLists) {
            String optType = opt.getString(OracleAttr.PERATEIONTYPE);

            String optSql = null;
            String optOwner = opt.getString(OracleAttr.OWNER);
            if (optOwner != null || StringUtils.isNotEmpty(optOwner.trim())) {
                optOwner = OracleAttr.CHANGE_OWNER;
            }
            StringBuilder optTableBuilder = new StringBuilder(optOwner);
            String optTable = optTableBuilder.append(".").append(opt.getString(OracleAttr.TABLE).toUpperCase()).toString();
            String optPK = opt.getString(OracleAttr.PRIMARYKEY);
            JSONObject optFiledValue = opt.getJSONObject(OracleAttr.FILEDVALUE);
            List<List> filedList = OracleParser.getFiledListHaveFilter(optFiledValue, optTable);
            List<List> primarykeyList = OracleParser.getPrimaryKeyListHaveFilter(optFiledValue, optPK, optType, optTable);
            if (OracleAttr.UPDATE.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToUpdateSql(filedList, primarykeyList, optTable);
            } else if (OracleAttr.INSERT.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToInsertSql(filedList, optTable);
            } else if (OracleAttr.DELETE.equalsIgnoreCase(optType)) {
                optSql = OracleParser.jsonToDeleteSql(primarykeyList, optTable);
            } else if (OracleAttr.UPDATEPK.equalsIgnoreCase(optType)) {
                //todo 更新PK
                log.error("Ucaccepted operation: update PK\n" + opt.toJSONString());
                continue;
            } else {
                log.error("Unaccepted operation:\n" + opt.toJSONString());
                continue;
            }

            JSONObject optSqlJson = new JSONObject();
            optSqlJson.put("opt", optType);
            optSqlJson.put("table", optTable);
            optSqlJson.put("sql", optSql);
            this.queue.add(optSqlJson);
        }

    }

    public static void main(String[] args) {
        BlockingQueue<JSONObject> queue = new LinkedBlockingQueue<JSONObject>();
        KafkaConsumer kafkaConsumer = new KafkaConsumer(queue);
        kafkaConsumer.consume("master", "S3SA048:2181,S3SA049:2181,S3SA050:2181", "test-gomewallet-group-1", "finance_gome_wallet", 1);
    }


}
