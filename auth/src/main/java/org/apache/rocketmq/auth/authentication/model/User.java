package org.apache.rocketmq.auth.authentication.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.auth.authentication.enums.SubjectType;
import org.apache.rocketmq.auth.authentication.enums.UserStatus;
import org.apache.rocketmq.auth.authentication.enums.UserType;
import org.apache.rocketmq.common.constant.CommonConstants;

@Getter
@Setter
public class User implements Subject {

    private String username;

    private String password;

    private UserType userType;

    private UserStatus userStatus;

    public static User of(String username) {
        User user = new User();
        user.setUsername(username);
        return user;
    }

    public static User of(String username, String password) {
        User user = new User();
        user.setUsername(username);
        user.setPassword(password);
        return user;
    }

    public static User of(String username, String password, UserType userType) {
        User user = of(username, password);
        user.setUserType(userType);
        return user;
    }

    @Override
    public String getSubjectKey() {
        return this.getSubjectType().getName() + CommonConstants.COLON + this.username;
    }

    @Override
    public SubjectType getSubjectType() {
        return SubjectType.USER;
    }

}