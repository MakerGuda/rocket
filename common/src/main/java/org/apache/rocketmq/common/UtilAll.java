package org.apache.rocketmq.common;

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.zip.CRC32;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class UtilAll {

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static final String YYYY_MM_DD_HH_MM_SS_SSS = "yyyy-MM-dd#HH:mm:ss:SSS";

    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final Logger STORE_LOG = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final static char[] HEX_ARRAY;

    private final static int PID;

    static {
        HEX_ARRAY = "0123456789ABCDEF".toCharArray();
        Supplier<Integer> supplier = () -> {
            String currentJVM = ManagementFactory.getRuntimeMXBean().getName();
            try {
                return Integer.parseInt(currentJVM.substring(0, currentJVM.indexOf('@')));
            } catch (Exception e) {
                return -1;
            }
        };
        PID = supplier.get();
    }

    public static int getPid() {
        return PID;
    }

    public static void sleep(long sleepMs) {
        sleep(sleepMs, TimeUnit.MILLISECONDS);
    }

    public static void sleep(long timeOut, TimeUnit timeUnit) {
        if (null == timeUnit) {
            return;
        }
        try {
            timeUnit.sleep(timeOut);
        } catch (Throwable ignore) {

        }
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    public static long computeElapsedTimeMilliseconds(final long beginTime) {
        return System.currentTimeMillis() - beginTime;
    }

    public static boolean isItTimeToDo(final String when) {
        String[] whiles = when.split(";");
        if (whiles.length > 0) {
            Calendar now = Calendar.getInstance();
            for (String w : whiles) {
                int nowHour = Integer.parseInt(w);
                if (nowHour == now.get(Calendar.HOUR_OF_DAY)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String timeMillisToHumanString(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
    }

    public static long computeNextMorningTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static long computeNextMinutesTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 0);
        cal.add(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static long computeNextHourTimeMillis() {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.add(Calendar.DAY_OF_MONTH, 0);
        cal.add(Calendar.HOUR_OF_DAY, 1);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

    public static String timeMillisToHumanString2(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d-%02d-%02d %02d:%02d:%02d,%03d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND), cal.get(Calendar.MILLISECOND));
    }

    public static String timeMillisToHumanString3(final long t) {
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(t);
        return String.format("%04d%02d%02d%02d%02d%02d", cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1, cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE), cal.get(Calendar.SECOND));
    }

    public static boolean isPathExists(final String path) {
        File file = new File(path);
        return file.exists();
    }

    public static double getDiskPartitionSpaceUsedPercent(final String path) {
        if (null == path || path.isEmpty()) {
            STORE_LOG.error("Error when measuring disk space usage, path is null or empty, path : {}", path);
            return -1;
        }
        try {
            File file = new File(path);
            if (!file.exists()) {
                STORE_LOG.error("Error when measuring disk space usage, file doesn't exist on this path: {}", path);
                return -1;
            }
            long totalSpace = file.getTotalSpace();
            if (totalSpace > 0) {
                long usedSpace = totalSpace - file.getFreeSpace();
                long usableSpace = file.getUsableSpace();
                long entireSpace = usedSpace + usableSpace;
                long roundNum = 0;
                if (usedSpace * 100 % entireSpace != 0) {
                    roundNum = 1;
                }
                long result = usedSpace * 100 / entireSpace + roundNum;
                return result / 100.0;
            }
        } catch (Exception e) {
            STORE_LOG.error("Error when measuring disk space usage, got exception: :", e);
            return -1;
        }
        return -1;
    }

    public static long getDiskPartitionTotalSpace(final String path) {
        if (null == path || path.isEmpty()) {
            return -1;
        }
        try {
            File file = new File(path);
            if (!file.exists()) {
                return -1;
            }
            return file.getTotalSpace() - file.getFreeSpace() + file.getUsableSpace();
        } catch (Exception e) {
            return -1;
        }
    }

    public static int crc32(byte[] array) {
        if (array != null) {
            return crc32(array, 0, array.length);
        }
        return 0;
    }

    public static int crc32(byte[] array, int offset, int length) {
        CRC32 crc32 = new CRC32();
        crc32.update(array, offset, length);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static int crc32(ByteBuffer byteBuffer) {
        CRC32 crc32 = new CRC32();
        crc32.update(byteBuffer);
        return (int) (crc32.getValue() & 0x7FFFFFFF);
    }

    public static String bytes2string(byte[] src) {
        char[] hexChars = new char[src.length * 2];
        for (int j = 0; j < src.length; j++) {
            int v = src[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void writeInt(char[] buffer, int pos, int value) {
        for (int moveBits = 28; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = HEX_ARRAY[(value >>> moveBits) & 0x0F];
        }
    }

    public static void writeShort(char[] buffer, int pos, int value) {
        for (int moveBits = 12; moveBits >= 0; moveBits -= 4) {
            buffer[pos++] = HEX_ARRAY[(value >>> moveBits) & 0x0F];
        }
    }

    public static byte[] string2bytes(String hexString) {
        if (hexString == null || hexString.isEmpty()) {
            return null;
        }
        hexString = hexString.toUpperCase();
        int length = hexString.length() / 2;
        char[] hexChars = hexString.toCharArray();
        byte[] d = new byte[length];
        for (int i = 0; i < length; i++) {
            int pos = i * 2;
            d[i] = (byte) (charToByte(hexChars[pos]) << 4 | charToByte(hexChars[pos + 1]));
        }
        return d;
    }

    private static byte charToByte(char c) {
        return (byte) "0123456789ABCDEF".indexOf(c);
    }

    @Deprecated
    public static byte[] uncompress(final byte[] src) throws IOException {
        byte[] result;
        byte[] uncompressData = new byte[src.length];
        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(src);
        InflaterInputStream inflaterInputStream = new InflaterInputStream(byteArrayInputStream);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        try {
            while (true) {
                int len = inflaterInputStream.read(uncompressData, 0, uncompressData.length);
                if (len <= 0) {
                    break;
                }
                byteArrayOutputStream.write(uncompressData, 0, len);
            }
            byteArrayOutputStream.flush();
            result = byteArrayOutputStream.toByteArray();
        } finally {
            try {
                byteArrayInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                inflaterInputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                log.error("Failed to close the stream", e);
            }
        }
        return result;
    }

    @Deprecated
    public static byte[] compress(final byte[] src, final int level) throws IOException {
        byte[] result;
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(src.length);
        java.util.zip.Deflater defeater = new java.util.zip.Deflater(level);
        DeflaterOutputStream deflaterOutputStream = new DeflaterOutputStream(byteArrayOutputStream, defeater);
        try {
            deflaterOutputStream.write(src);
            deflaterOutputStream.finish();
            deflaterOutputStream.close();
            result = byteArrayOutputStream.toByteArray();
        } catch (IOException e) {
            defeater.end();
            throw e;
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException ignore) {
            }
            defeater.end();
        }
        return result;
    }

    public static String formatDate(Date date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        return df.format(date);
    }

    public static Date parseDate(String date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern);
        try {
            return df.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    public static String responseCode2String(final int code) {
        return Integer.toString(code);
    }

    public static String frontStringAtLeast(final String str, final int size) {
        if (str != null) {
            if (str.length() > size) {
                return str.substring(0, size);
            }
        }
        return str;
    }

    public static boolean isBlank(String str) {
        return StringUtils.isBlank(str);
    }

    public static String jstack() {
        return jstack(Thread.getAllStackTraces());
    }

    public static String jstack(Map<Thread, StackTraceElement[]> map) {
        StringBuilder result = new StringBuilder();
        try {
            for (Map.Entry<Thread, StackTraceElement[]> entry : map.entrySet()) {
                StackTraceElement[] elements = entry.getValue();
                Thread thread = entry.getKey();
                if (elements != null && elements.length > 0) {
                    String threadName = entry.getKey().getName();
                    result.append(String.format("%-40sTID: %d STATE: %s%n", threadName, thread.getId(), thread.getState()));
                    for (StackTraceElement el : elements) {
                        result.append(String.format("%-40s%s%n", threadName, el.toString()));
                    }
                    result.append("\n");
                }
            }
        } catch (Throwable e) {
            result.append(exceptionSimpleDesc(e));
        }
        return result.toString();
    }

    public static String exceptionSimpleDesc(final Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (e != null) {
            sb.append(e);
            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement element = stackTrace[0];
                sb.append(", ");
                sb.append(element.toString());
            }
        }
        return sb.toString();
    }

    public static boolean isInternalIP(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }
        if (ip[0] == (byte) 10) {
            return true;
        } else if (ip[0] == (byte) 127) {
            return true;
        } else if (ip[0] == (byte) 172) {
            return ip[1] >= (byte) 16 && ip[1] <= (byte) 31;
        } else if (ip[0] == (byte) 192) {
            return ip[1] == (byte) 168;
        }
        return false;
    }

    public static boolean isInternalV6IP(InetAddress inetAddr) {
        return inetAddr.isAnyLocalAddress() || inetAddr.isLinkLocalAddress() || inetAddr.isLoopbackAddress() || inetAddr.isSiteLocalAddress();
    }

    private static boolean ipCheck(byte[] ip) {
        if (ip.length != 4) {
            throw new RuntimeException("illegal ipv4 bytes");
        }
        InetAddressValidator validator = InetAddressValidator.getInstance();
        return validator.isValidInet4Address(ipToIPv4Str(ip));
    }

    private static boolean ipV6Check(byte[] ip) {
        if (ip.length != 16) {
            throw new RuntimeException("illegal ipv6 bytes");
        }
        InetAddressValidator validator = InetAddressValidator.getInstance();
        return validator.isValidInet6Address(ipToIPv6Str(ip));
    }

    public static String ipToIPv4Str(byte[] ip) {
        if (ip.length != 4) {
            return null;
        }
        return (ip[0] & 0xFF) + "." + (ip[1] & 0xFF) + "." + (ip[2] & 0xFF) + "." + (ip[3] & 0xFF);
    }

    public static String ipToIPv6Str(byte[] ip) {
        if (ip.length != 16) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ip.length; i++) {
            String hex = Integer.toHexString(ip[i] & 0xFF);
            if (hex.length() < 2) {
                sb.append(0);
            }
            sb.append(hex);
            if (i % 2 == 1 && i < ip.length - 1) {
                sb.append(":");
            }
        }
        return sb.toString();
    }

    public static byte[] getIP() {
        try {
            Enumeration<?> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            InetAddress ip;
            byte[] internalIP = null;
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
                Enumeration<?> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    ip = (InetAddress) addresses.nextElement();
                    if (ip instanceof Inet4Address) {
                        byte[] ipByte = ip.getAddress();
                        if (ipByte.length == 4) {
                            if (ipCheck(ipByte)) {
                                if (!isInternalIP(ipByte)) {
                                    return ipByte;
                                } else if (internalIP == null || internalIP[0] == (byte) 127) {
                                    internalIP = ipByte;
                                }
                            }
                        }
                    } else if (ip instanceof Inet6Address) {
                        byte[] ipByte = ip.getAddress();
                        if (ipByte.length == 16) {
                            if (ipV6Check(ipByte)) {
                                if (!isInternalV6IP(ip)) {
                                    return ipByte;
                                }
                            }
                        }
                    }
                }
            }
            if (internalIP != null) {
                return internalIP;
            } else {
                throw new RuntimeException("Can not get local ip");
            }
        } catch (Exception e) {
            throw new RuntimeException("Can not get local ip", e);
        }
    }

    public static void deleteFile(File file) {
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
        } else if (file.isDirectory()) {
            File[] files = file.listFiles();
            assert files != null;
            for (File file1 : files) {
                deleteFile(file1);
            }
            file.delete();
        }
    }

    public static String join(List<String> list, String splitter) {
        if (list == null) {
            return null;
        }
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < list.size(); i++) {
            str.append(list.get(i));
            if (i == list.size() - 1) {
                break;
            }
            str.append(splitter);
        }
        return str.toString();
    }

    public static List split(String str, String splitter) {
        if (str == null) {
            return null;
        }
        if (StringUtils.isBlank(str)) {
            return Collections.EMPTY_LIST;
        }
        String[] addrArray = str.split(splitter);
        return Arrays.asList(addrArray);
    }

    public static void deleteEmptyDirectory(File file) {
        if (file == null || !file.exists()) {
            return;
        }
        if (!file.isDirectory()) {
            return;
        }
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            file.delete();
            STORE_LOG.info("delete empty direct, {}", file.getPath());
        }
    }

    public static void cleanBuffer(final ByteBuffer buffer) {
        if (null == buffer) {
            return;
        }
        if (!buffer.isDirect()) {
            return;
        }
        PlatformDependent.freeDirectBuffer(buffer);
    }

    public static void ensureDirOK(final String dirName) {
        if (dirName != null) {
            if (dirName.contains(MixAll.MULTI_PATH_SPLITTER)) {
                String[] dirs = dirName.trim().split(MixAll.MULTI_PATH_SPLITTER);
                for (String dir : dirs) {
                    createDirIfNotExist(dir);
                }
            } else {
                createDirIfNotExist(dirName);
            }
        }
    }

    private static void createDirIfNotExist(String dirName) {
        File f = new File(dirName);
        if (!f.exists()) {
            boolean result = f.mkdirs();
            STORE_LOG.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
        }
    }

    public static long calculateFileSizeInPath(File path) {
        long size = 0;
        try {
            if (!path.exists() || Files.isSymbolicLink(path.toPath())) {
                return 0;
            }
            if (path.isFile()) {
                return path.length();
            }
            if (path.isDirectory()) {
                File[] files = path.listFiles();
                if (files != null) {
                    for (File file : files) {
                        long fileSize = calculateFileSizeInPath(file);
                        if (fileSize == -1) return -1;
                        size += fileSize;
                    }
                }
            }
        } catch (Exception e) {
            log.error("calculate all file size in: {} error", path.getAbsolutePath(), e);
            return -1;
        }
        return size;
    }

}