package org.apache.rocketmq.common.utils;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class DataConverter {

    public static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    public static int setBit(int value, int index, boolean flag) {
        if (flag) {
            return (int) (value | (1L << index));
        } else {
            return (int) (value & ~(1L << index));
        }
    }

    public static boolean getBit(int value, int index) {
        return (value & (1L << index)) != 0;
    }

}