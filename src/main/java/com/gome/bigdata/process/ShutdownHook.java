package com.gome.bigdata.process;

import com.gome.bigdata.main.OracleEntry;
import org.apache.log4j.Logger;

/**
 * Created by lujia on 2015/4/2.
 */
public class ShutdownHook {
    private static Logger log = Logger.getLogger(ShutdownHook.class.getName());
    public ShutdownHook(final OracleEntry oracleEntry){
        log.info("-------Start ShutdownHook----------");
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                oracleEntry.stop();
                log.info("-----ShutdowHook stop-------------");
            }
        });
    }
}
