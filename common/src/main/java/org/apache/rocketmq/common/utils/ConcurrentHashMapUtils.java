package org.apache.rocketmq.common.utils;

import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public abstract class ConcurrentHashMapUtils {

    private static boolean isJdk8;

    static {
        try {
            isJdk8 = System.getProperty("java.version").startsWith("1.8.");
        } catch (Exception ignore) {
            isJdk8 = true;
        }
    }

    public static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Function<? super K, ? extends V> func) {
        Objects.requireNonNull(func);
        if (isJdk8) {
            V v = map.get(key);
            if (null == v) {
                v = func.apply(key);
                if (null == v) {
                    return null;
                }
                final V res = map.putIfAbsent(key, v);
                if (null != res) {
                    return res;
                }
            }
            return v;
        } else {
            return map.computeIfAbsent(key, func);
        }
    }

}