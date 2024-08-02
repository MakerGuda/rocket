package org.apache.rocketmq.common.compression;

import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Lz4Compressor implements Compressor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    @Override
    public byte[] compress(byte[] src, int level) throws IOException {
        byte[] result;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        LZ4FrameOutputStream outputStream = new LZ4FrameOutputStream(byteArrayOutputStream);
        try {
            outputStream.write(src);
            outputStream.flush();
            outputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            log.error("Failed to compress data by lz4", e);
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignored) {
            }
        }
        return result;
    }

    @Override
    public byte[] decompress(byte[] src) throws IOException {
        byte[] result;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        LZ4FrameInputStream lz4InputStream = new LZ4FrameInputStream(byteArrayInputStream);
        ByteArrayOutputStream resultOutputStream = new ByteArrayOutputStream(src.length);
        try {
            while (true) {
                int len = lz4InputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                resultOutputStream.write(uncompressData, 0, len);
            }
            resultOutputStream.flush();
            resultOutputStream.close();
            result = resultOutputStream.toByteArray();
        } finally {
            try {
                lz4InputStream.close();
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.warn("Failed to close the lz4 compress stream ", e);
            }
        }
        return result;
    }

}