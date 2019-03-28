package com.jd.journalkeeper.utils.format;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

/**
 * @author liyue25
 * Date: 2019-01-17
 */
public class Format {
    private static ThreadLocal<SimpleDateFormat> sdfHolder
            = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    public static String format(Date date) {
        return sdfHolder.get().format(date);
    }

    public static String formatWithComma(long position) {
        return  NumberFormat.getNumberInstance(Locale.US).format(position);
    }

    /**
     * copied from https://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc/5599842#5599842
     */
    public static String formatTraffic(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }
}
