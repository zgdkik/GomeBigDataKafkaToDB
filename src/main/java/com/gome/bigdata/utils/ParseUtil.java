package com.gome.bigdata.utils;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by lujia on 2015/5/27.
 */
public class ParseUtil {

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

}
