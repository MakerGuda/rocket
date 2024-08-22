package org.apache.rocketmq.tools.command.message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.tools.command.SubCommand;
import org.apache.rocketmq.tools.command.SubCommandException;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class DumpCompactionLogCommand implements SubCommand {

    @Override
    public String commandDesc() {
        return "Parse compaction log to message.";
    }

    @Override
    public String commandName() {
        return "dumpCompactionLog";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("f", "file", true, "to dump file name");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options, RPCHook rpcHook) throws SubCommandException {
        if (commandLine.hasOption("f")) {
            String fileName = commandLine.getOptionValue("f");
            Path filePath = Paths.get(fileName);
            if (!Files.exists(filePath)) {
                throw new SubCommandException("file " + fileName + " not exist.");
            }
            if (Files.isDirectory(filePath)) {
                throw new SubCommandException("file " + fileName + " is a directory.");
            }
            try {
                long fileSize = Files.size(filePath);
                FileChannel fileChannel = new RandomAccessFile(fileName, "rw").getChannel();
                ByteBuffer buf = fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                int current = 0;
                while (current < fileSize) {
                    buf.position(current);
                    ByteBuffer bb = buf.slice();
                    int size = bb.getInt();
                    if (size > buf.capacity() || size < 0) {
                        break;
                    } else {
                        bb.limit(size);
                        bb.rewind();
                    }
                    try {
                        MessageExt messageExt = MessageDecoder.decode(bb, false, false);
                        if (messageExt == null) {
                            break;
                        } else {
                            current += size;
                            System.out.printf(messageExt + "\n");
                        }
                    } catch (Exception ignore) {
                    }
                }
                UtilAll.cleanBuffer(buf);
            } catch (IOException ignore) {
            }
        } else {
            System.out.print("miss dump log file name\n");
        }
    }

}