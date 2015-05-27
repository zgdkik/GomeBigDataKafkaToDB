package com.gome.bigdata.attr;

/**
 * Created by lujia on 2015/5/22.
 */
public class ConfAttr {

    /**
     * 写入oracle前的BQ的大小
     */
    public static int BQ_BUFFER_SIZE = 100000;

    /**
     * 程序内存监控文件地址
     */
    public static String BUFFER_MONITOR_FILE = "logs/monitor.txt";

    /**
     * 统计每小时的接收和保存数量
     */
    public static String HOUR_RESET_COUNT_FILE = "logs/hourCount.txt";

    /**
     * 内存监控的时间间隔，秒
     */
    public static int MEM_MONITOR_SECONDS = 10;

}
