package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.help.FAQUrl;

@Getter
@Setter
public class AbortProcessException extends RuntimeException {

    private static final long serialVersionUID = -5728810933841185841L;

    private int responseCode;

    private String errorMessage;

    public AbortProcessException(int responseCode, String errorMessage) {
        super(FAQUrl.attachDefaultURL("CODE: " + UtilAll.responseCode2String(responseCode) + "  DESC: " + errorMessage));
        this.responseCode = responseCode;
        this.errorMessage = errorMessage;
    }

}