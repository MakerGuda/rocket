package org.apache.rocketmq.common.utils;

import org.apache.commons.lang3.SerializationException;

public interface Serializer {

    <T> byte[] serialize(T t) throws SerializationException;

    <T> T deserialize(byte[] bytes, Class<T> type) throws SerializationException;

}