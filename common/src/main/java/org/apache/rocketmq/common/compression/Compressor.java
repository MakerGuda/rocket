package org.apache.rocketmq.common.compression;

import java.io.IOException;

public interface Compressor {

    /**
     * 压缩消息
     *
     * @param src 待压缩的消息
     * @param level 压缩级别
     */
    byte[] compress(byte[] src, int level) throws IOException;

    /**
     * 解压缩消息
     *
     * @param src 待解压缩的消息
     */
    byte[] decompress(byte[] src) throws IOException;

}