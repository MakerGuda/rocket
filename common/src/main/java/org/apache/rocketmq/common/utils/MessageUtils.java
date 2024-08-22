package org.apache.rocketmq.common.utils;

import static org.apache.rocketmq.common.message.MessageDecoder.NAME_VALUE_SEPARATOR;
import static org.apache.rocketmq.common.message.MessageDecoder.PROPERTY_SEPARATOR;

public class MessageUtils {

    public static String deleteProperty(String propertiesString, String name) {
        if (propertiesString != null) {
            int idx0 = 0;
            int idx1;
            int idx2;
            idx1 = propertiesString.indexOf(name, idx0);
            if (idx1 != -1) {
                StringBuilder stringBuilder = new StringBuilder(propertiesString.length());
                while (true) {
                    int startIdx = idx0;
                    while (true) {
                        idx1 = propertiesString.indexOf(name, startIdx);
                        if (idx1 == -1) {
                            break;
                        }
                        startIdx = idx1 + name.length();
                        if (idx1 == 0 || propertiesString.charAt(idx1 - 1) == PROPERTY_SEPARATOR) {
                            if (propertiesString.length() > idx1 + name.length() && propertiesString.charAt(idx1 + name.length()) == NAME_VALUE_SEPARATOR) {
                                break;
                            }
                        }
                    }
                    if (idx1 == -1) {
                        stringBuilder.append(propertiesString, idx0, propertiesString.length());
                        break;
                    }
                    stringBuilder.append(propertiesString, idx0, idx1);
                    idx2 = propertiesString.indexOf(PROPERTY_SEPARATOR, idx1 + name.length() + 1);
                    if (idx2 == -1) {
                        break;
                    }
                    idx0 = idx2 + 1;
                }
                return stringBuilder.toString();
            }
        }
        return propertiesString;
    }

}