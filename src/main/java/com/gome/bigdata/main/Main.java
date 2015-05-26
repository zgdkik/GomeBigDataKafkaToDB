package com.gome.bigdata.main;

/**
 * Created by lujia on 2015/5/22.
 */
public class Main {

    public static void main(String[] args) {

        OracleEntry oracleEntry = new OracleEntry();
        oracleEntry.initConfig();
        oracleEntry.startSaveToOracleExecutor(2);
        oracleEntry.startBufferMonitor(10);
        oracleEntry.startKafkaConsumer("smallest", "S3SA048:2181,S3SA049:2181,S3SA050:2181","sso-pre-test1","sso_mysql_to_oracle_uat_v1",1);


    }
}
