package com.gome.bigdata.main;

import com.gome.bigdata.process.ShutdownHook;
import com.gome.bigdata.utils.PropertiesUtil;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Created by lujia on 2015/5/22.
 */
public class Main {
    private static Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        //TODO 写死在程序中的配置文件，log4j
        PropertyConfigurator.configure(PropertiesUtil.getInstance().getProperty("log4j.properties"));
        //检查启动参数
        if (args.length < 5) {
            System.err.println("Usage: JavaKafkaToHbase <offsetReset> <zkQuorum> <group> <topicStr> <numthreads>");
            log.error("Init Failer because of lacking parameters...");
            System.exit(1);
        }
        String offsetReset = args[0];
        String zkQuorum = args[1];
        String group = args[2];
        String topic = args[3];
        int numThread = Integer.valueOf(args[4]);

        OracleEntry oracleEntry = new OracleEntry();
        ShutdownHook shutdownHook = new ShutdownHook(oracleEntry);


        oracleEntry.initConfig();
        oracleEntry.startOracleEntry();
        oracleEntry.startKafkaConsumer(offsetReset, zkQuorum, group, topic, numThread);
//        oracleEntry.startKafkaConsumer("smallest", "S3SA048:2181,S3SA049:2181,S3SA050:2181","sso-pre-test1","sso_mysql_to_oracle_uat_v1",1);
    }
}
