package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
public class PlainAccessConfig implements Serializable {

    private static final long serialVersionUID = -4517357000307227637L;

    private String accessKey;

    private String secretKey;

    private String whiteRemoteAddress;

    private boolean admin;

    private String defaultTopicPerm;

    private String defaultGroupPerm;

    private List<String> topicPerms;

    private List<String> groupPerms;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PlainAccessConfig config = (PlainAccessConfig) o;
        return admin == config.admin && Objects.equals(accessKey, config.accessKey) && Objects.equals(secretKey, config.secretKey) && Objects.equals(whiteRemoteAddress, config.whiteRemoteAddress) && Objects.equals(defaultTopicPerm, config.defaultTopicPerm) && Objects.equals(defaultGroupPerm, config.defaultGroupPerm) && Objects.equals(topicPerms, config.topicPerms) && Objects.equals(groupPerms, config.groupPerms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessKey, secretKey, whiteRemoteAddress, admin, defaultTopicPerm, defaultGroupPerm, topicPerms, groupPerms);
    }

}