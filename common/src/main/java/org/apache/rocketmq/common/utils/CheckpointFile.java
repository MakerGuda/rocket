package org.apache.rocketmq.common.utils;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry Checkpoint file util
 * Format:
 * <li>First line:  Entries size
 * <li>Second line: Entries crc32
 * <li>Next: Entry data per line
 * <p>
 * Example:
 * <li>2 (size)
 * <li>773307083 (crc32)
 * <li>7-7000 (entry data)
 * <li>8-8000 (entry data)
 */
public class CheckpointFile<T> {

    /**
     * Not check crc32 when value is 0
     */
    private static final int NOT_CHECK_CRC_MAGIC_CODE = 0;

    private final String filePath;

    private final CheckpointSerializer<T> serializer;

    public CheckpointFile(final String filePath, final CheckpointSerializer<T> serializer) {
        this.filePath = filePath;
        this.serializer = serializer;
    }

    public String getBackFilePath() {
        return this.filePath + ".bak";
    }

    public void write(final List<T> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }
        synchronized (this) {
            StringBuilder entryContent = new StringBuilder();
            for (T entry : entries) {
                final String line = this.serializer.toLine(entry);
                if (line != null && !line.isEmpty()) {
                    entryContent.append(line);
                    entryContent.append(System.lineSeparator());
                }
            }
            int crc32 = UtilAll.crc32(entryContent.toString().getBytes(StandardCharsets.UTF_8));
            String content = entries.size() + System.lineSeparator() + crc32 + System.lineSeparator() + entryContent;
            MixAll.string2File(content, this.filePath);
        }
    }

    private List<T> read(String filePath) throws IOException {
        final ArrayList<T> result = new ArrayList<>();
        synchronized (this) {
            final File file = new File(filePath);
            if (!file.exists()) {
                return result;
            }
            try (BufferedReader reader = Files.newBufferedReader(file.toPath())) {
                int expectedLines = Integer.parseInt(reader.readLine());
                int expectedCrc32 = Integer.parseInt(reader.readLine());
                StringBuilder sb = new StringBuilder();
                String line = reader.readLine();
                while (line != null) {
                    sb.append(line).append(System.lineSeparator());
                    final T entry = this.serializer.fromLine(line);
                    if (entry != null) {
                        result.add(entry);
                    }
                    line = reader.readLine();
                }
                int truthCrc32 = UtilAll.crc32(sb.toString().getBytes(StandardCharsets.UTF_8));
                if (result.size() != expectedLines) {
                    final String err = String.format("Expect %d entries, only found %d entries", expectedLines, result.size());
                    throw new IOException(err);
                }
                if (NOT_CHECK_CRC_MAGIC_CODE != expectedCrc32 && truthCrc32 != expectedCrc32) {
                    final String err = String.format("Entries crc32 not match, file=%s, truth=%s", expectedCrc32, truthCrc32);
                    throw new IOException(err);
                }
                return result;
            }
        }
    }

    public List<T> read() throws IOException {
        try {
            List<T> result = this.read(this.filePath);
            if (CollectionUtils.isEmpty(result)) {
                result = this.read(this.getBackFilePath());
            }
            return result;
        } catch (IOException e) {
            return this.read(this.getBackFilePath());
        }
    }

    public interface CheckpointSerializer<T> {

        String toLine(final T entry);

        T fromLine(final String line);

    }

}