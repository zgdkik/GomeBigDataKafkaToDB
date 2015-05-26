package com.gome.bigdata.monitor;

import com.alibaba.fastjson.JSONObject;
import com.gome.bigdata.main.OracleEntry;
import org.apache.log4j.Logger;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;

/**
 * Created by lujia on 2015/5/26.
 */
public class OracleSqlBufferMonitor extends TimerTask {
    private static Logger log = Logger.getLogger(OracleSqlBufferMonitor.class);

    private Timer monitorTimer = null;
    private final BlockingQueue<JSONObject> oracleSqlBuffer;

    public OracleSqlBufferMonitor(BlockingQueue<JSONObject> oracleSqlBuffer) {
        monitorTimer = new Timer();
        this.oracleSqlBuffer = oracleSqlBuffer;
    }

    @Override
    public void run() {
        Date time = new Date();
        String countline = String.format(
                "%s -This hour,Received count: %d, Saved count: %d; Buffer Size: %d  \n\n", time.toString(), OracleEntry.getReceivedFromKafkaOptCount(), OracleEntry.getSaveToOracleSuccessCount(), oracleSqlBuffer.size());
        System.out.println(countline);

    }

    /**
     * 监控buffer
     *
     * @param delay    , 延迟, 毫秒
     * @param internal , s 秒
     */
    public void start(long delay, int internal) {
        monitorTimer.schedule(this, delay, internal * 1000); // 利用timer.schedule方法
    }

    public void end() {
        monitorTimer.cancel();
    }
}
