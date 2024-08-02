package org.apache.rocketmq.common.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

public class IOTinyUtils {

    static public String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, StandardCharsets.UTF_8)) : toString(new InputStreamReader(input, encoding));
    }

    static public String toString(Reader reader) throws IOException {
        CharArrayWriter sw = new CharArrayWriter();
        copy(reader, sw);
        return sw.toString();
    }

    static public long copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[1 << 12];
        long count = 0;
        for (int n; (n = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    public static void delete(File fileOrDir) throws IOException {
        if (fileOrDir == null) {
            return;
        }
        if (fileOrDir.isDirectory()) {
            cleanDirectory(fileOrDir);
        }
        fileOrDir.delete();
    }

    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }
        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }
        File[] files = directory.listFiles();
        if (files == null) {
            throw new IOException("Failed to list contents of " + directory);
        }
        IOException exception = null;
        for (File file : files) {
            try {
                delete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }
        if (null != exception) {
            throw exception;
        }
    }

    public static void writeStringToFile(File file, String data, String encoding) throws IOException {
        try (OutputStream os = Files.newOutputStream(file.toPath())) {
            os.write(data.getBytes(encoding));
        }
    }

}