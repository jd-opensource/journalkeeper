package com.jd.journalkeeper.coordinating.server.util;

import java.util.Properties;

/**
 * PropertiesUtils
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/4
 */
public class PropertiesUtils {

    public static String getProperty(Properties properties, String key, Object defaultValue) {
        return doGetProperty(properties, key, defaultValue);
    }

    public static String getProperty(Properties properties, String key, String defaultValue) {
        return doGetProperty(properties, key, defaultValue);
    }

    public static short getProperty(Properties properties, String key, short defaultValue) {
        return Short.valueOf(doGetProperty(properties, key, defaultValue));
    }

    public static byte getProperty(Properties properties, String key, byte defaultValue) {
        return Byte.valueOf(doGetProperty(properties, key, defaultValue));
    }

    public static double getProperty(Properties properties, String key, double defaultValue) {
        return Double.valueOf(doGetProperty(properties, key, defaultValue));
    }

    public static long getProperty(Properties properties, String key, long defaultValue) {
        return Long.valueOf(doGetProperty(properties, key, defaultValue));
    }

    public static int getProperty(Properties properties, String key, int defaultValue) {
        return Integer.valueOf(doGetProperty(properties, key, defaultValue));
    }

    public static boolean getProperty(Properties properties, String key, boolean defaultValue) {
        return Boolean.valueOf(doGetProperty(properties, key, defaultValue));
    }

    protected static String doGetProperty(Properties properties, String key, Object defaultValue) {
        if (properties == null) {
            return null;
        }
        String value = properties.getProperty(key);
        if (value == null) {
            return String.valueOf(defaultValue);
        }
        return value;
    }
}