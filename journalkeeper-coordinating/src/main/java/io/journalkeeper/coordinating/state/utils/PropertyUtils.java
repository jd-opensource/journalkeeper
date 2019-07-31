/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.coordinating.state.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * PropertyUtils
 * author: gaohaoxiang
 *
 * date: 2019/6/10
 */
public class PropertyUtils {

    public static Object convert(String value, Class<?> type) {
        if (type.equals(int.class) || type.equals(Integer.class)) {
            return convertInt(value, 0);
        } else if (type.equals(long.class) || type.equals(Long.class)) {
            return convertLong(value, 0);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            return convertShort(value, (short) 0);
        } else if (type.equals(String.class)) {
            return convertString(value, "");
        } else if (type.equals(char.class)) {
            return convertChar(value, (char) 0);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            return convertBoolean(value, false);
        } else if (type.equals(byte.class) || type.equals(Byte.class)) {
            return convertByte(value, (byte) 0);
        } else if (type.equals(float.class) || type.equals(Float.class)) {
            return convertFloat(value, (float) 0);
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            return convertDouble(value, (double) 0);
        }
        return null;
    }

    public static int convertInt(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Integer.valueOf(value);
    }

    public static long convertLong(String value, long defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Long.valueOf(value);
    }

    public static short convertShort(String value, short defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Short.valueOf(value);
    }

    public static String convertString(String value, String defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return value;
    }

    public static boolean convertBoolean(String value, boolean defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Boolean.valueOf(value);
    }

    public static byte convertByte(String value, byte defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Byte.valueOf(value);
    }

    public static float convertFloat(String value, float defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Float.valueOf(value);
    }

    public static double convertDouble(String value, double defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return Double.valueOf(value);
    }

    public static char convertChar(String value, char defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        return value.charAt(0);
    }
}