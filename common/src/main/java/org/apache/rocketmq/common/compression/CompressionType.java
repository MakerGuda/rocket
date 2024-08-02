package org.apache.rocketmq.common.compression;

import lombok.Getter;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

@Getter
public enum CompressionType {

    /**
     *    Compression types number can be extended to seven {@link MessageSysFlag}
     *
     *    Benchmarks from <a href="https://github.com/facebook/zstd">...</a>
     *    |   Compressor   |  Ratio  | Compression | Decompress |
     *    |----------------|---------|-------------|------------|
     *    |   zstd 1.5.1   |  2.887  |   530 MB/s  |  1700 MB/s |
     *    |  zlib 1.2.11   |  2.743  |    95 MB/s  |   400 MB/s |
     *    |    lz4 1.9.3   |  2.101  |   740 MB/s  |  4500 MB/s |
     *
     */

    LZ4(1), ZSTD(2), ZLIB(3);

    private final int value;

    CompressionType(int value) {
        this.value = value;
    }

    public static CompressionType of(String name) {
        switch (name.trim().toUpperCase()) {
            case "LZ4":
                return CompressionType.LZ4;
            case "ZSTD":
                return CompressionType.ZSTD;
            case "ZLIB":
                return CompressionType.ZLIB;
            default:
                throw new RuntimeException("Unsupported compress type name: " + name);
        }
    }

    public static CompressionType findByValue(int value) {
        switch (value) {
            case 1:
                return LZ4;
            case 2:
                return ZSTD;
            case 0:
            case 3:
                return ZLIB;
            default:
                throw new RuntimeException("Unknown compress type value: " + value);
        }
    }

    public int getCompressionFlag() {
        switch (value) {
            case 1:
                return MessageSysFlag.COMPRESSION_LZ4_TYPE;
            case 2:
                return MessageSysFlag.COMPRESSION_ZSTD_TYPE;
            case 3:
                return MessageSysFlag.COMPRESSION_ZLIB_TYPE;
            default:
                throw new RuntimeException("Unsupported compress type flag: " + value);
        }
    }

}