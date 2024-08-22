package org.apache.rocketmq.common.compression;

import java.util.EnumMap;

/**
 * 压缩处理器工厂
 */
public class CompressorFactory {

    private static final EnumMap<CompressionType, Compressor> COMPRESSORS;

    static {
        COMPRESSORS = new EnumMap<>(CompressionType.class);
        COMPRESSORS.put(CompressionType.LZ4, new Lz4Compressor());
        COMPRESSORS.put(CompressionType.ZSTD, new ZstdCompressor());
        COMPRESSORS.put(CompressionType.ZLIB, new ZlibCompressor());
    }

    public static Compressor getCompressor(CompressionType type) {
        return COMPRESSORS.get(type);
    }

}