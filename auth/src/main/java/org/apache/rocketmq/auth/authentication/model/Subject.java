package org.apache.rocketmq.auth.authentication.model;

import com.alibaba.fastjson2.annotation.JSONField;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.common.constant.CommonConstants;

public interface Subject {

    @SuppressWarnings("unchecked")
    static <T extends Subject> T of(String subjectKey) {
        String type = StringUtils.substringBefore(subjectKey, CommonConstants.COLON);
        SubjectType subjectType = SubjectType.getByName(type);
        if (subjectType == null) {
            return null;
        }
        if (subjectType == SubjectType.USER) {
            return (T) User.of(StringUtils.substringAfter(subjectKey, CommonConstants.COLON));
        }
        return null;
    }

    @JSONField(serialize = false)
    String getSubjectKey();

    SubjectType getSubjectType();

    default boolean isSubject(SubjectType subjectType) {
        return subjectType == this.getSubjectType();
    }

}