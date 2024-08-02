package org.apache.rocketmq.common.help;

public class FAQUrl {

    public static final String APPLY_TOPIC_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String NAME_SERVER_ADDR_NOT_EXIST_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String GROUP_NAME_DUPLICATE_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String CLIENT_PARAMETER_CHECK_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SUBSCRIPTION_GROUP_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String CLIENT_SERVICE_NOT_OK = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String NO_TOPIC_ROUTE_INFO = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String LOAD_JSON_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SAME_GROUP_DIFFERENT_TOPIC = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String MQ_LIST_NOT_EXIST = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String UNEXPECTED_EXCEPTION_URL = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String SEND_MSG_FAILED = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    public static final String UNKNOWN_HOST_EXCEPTION = "https://rocketmq.apache.org/docs/bestPractice/06FAQ";

    private static final String TIP_STRING_BEGIN = "\nSee ";

    private static final String TIP_STRING_END = " for further details.";

    private static final String MORE_INFORMATION = "For more information, please visit the url, ";

    public static String suggestTodo(final String url) {
        return TIP_STRING_BEGIN + url + TIP_STRING_END;
    }

    public static String attachDefaultURL(final String errorMessage) {
        if (errorMessage != null) {
            int index = errorMessage.indexOf(TIP_STRING_BEGIN);
            if (-1 == index) {
                return errorMessage + "\n" + MORE_INFORMATION + UNEXPECTED_EXCEPTION_URL;
            }
        }
        return errorMessage;
    }

}