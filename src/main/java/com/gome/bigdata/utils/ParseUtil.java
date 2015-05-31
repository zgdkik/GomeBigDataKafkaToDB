package com.gome.bigdata.utils;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by lujia on 2015/5/27.
 */
public class ParseUtil {

    private static Logger log = Logger.getLogger(ParseUtil.class.getName());

    /**
     * 获取接下来第一个整点时间，long
     * @return long
     */
    public static long getFirstClockLong() {
        Calendar cal = Calendar.getInstance();
        Date day = new Date();
        cal.setTime(day);
        cal.set(Calendar.MINUTE, cal.getMaximum(Calendar.MINUTE));
        cal.set(Calendar.SECOND, cal.getMaximum(Calendar.SECOND));
        cal.set(Calendar.MILLISECOND, cal.getMaximum(Calendar.MILLISECOND));
        return cal.getTime().getTime() + 1;
    }

    /**
     * 记录执行错误的数据
     * @param path 保存路径
     * @param data 保存数据
     */
    public static void recordErrorSql(String path,String data) {
        FileWriter fileWritter = null;
        try {
            File file = new File(path);
            fileWritter = new FileWriter(file, true);
            fileWritter.write(data);
            fileWritter.flush();
            fileWritter.close();

        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            try {
                fileWritter.close();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }


}
