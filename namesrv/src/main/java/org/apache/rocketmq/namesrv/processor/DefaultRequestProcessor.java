package org.apache.rocketmq.namesrv.processor;

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MQVersion.Version;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.*;
import org.apache.rocketmq.remoting.protocol.body.*;
import org.apache.rocketmq.remoting.protocol.header.GetBrokerMemberGroupRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.*;
import org.apache.rocketmq.remoting.protocol.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultRequestProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    protected final NamesrvController namesrvController;

    protected Set<String> configBlackList = new HashSet<>();

    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        initConfigBlackList();
    }

    private void initConfigBlackList() {
        configBlackList.add("configBlackList");
        configBlackList.add("configStorePath");
        configBlackList.add("kvConfigPath");
        configBlackList.add("rocketmqHome");
        String[] configArray = namesrvController.getNamesrvConfig().getConfigBlackList().split(";");
        configBlackList.addAll(Arrays.asList(configArray));
    }

    /**
     * 处理请求
     */
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        if (ctx != null) {
            log.debug("receive request, {} {} {}", request.getCode(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), request);
        }
        switch (request.getCode()) {
            //设置k-v键值对
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(request);
            //获取k-v键值对
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(request);
            //删除键值对
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(request);
            //查询数据版本
            case RequestCode.QUERY_DATA_VERSION:
                return this.queryBrokerTopicConfig(request);
            //注册broker
            case RequestCode.REGISTER_BROKER:
                return this.registerBroker(ctx, request);
            //注销broker
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(request);
            //broker心跳检测
            case RequestCode.BROKER_HEARTBEAT:
                return this.brokerHeartbeat(request);
            //获取指定broker名称下的broker成员列表
            case RequestCode.GET_BROKER_MEMBER_GROUP:
                return this.getBrokerMemberGroup(request);
            //获取集群信息
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo();
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.ADD_WRITE_PERM_OF_BROKER:
                return this.addWritePermOfBroker(request);
            //从namesrv获取主题列表
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return this.getAllTopicListFromNameserver();
            //删除namesrv中的指定主题
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return this.deleteTopicInNamesrv(request);
            //往namesrv中注册主题
            case RequestCode.REGISTER_TOPIC_IN_NAMESRV:
                return this.registerTopicToNamesrv(request);
            //获取namespace下的kv列表
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(request);
            //获取集群下的主题列表
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(request);
            //获取系统主题列表
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs();
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList();
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList();
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList();
            //更新namesrv配置
            case RequestCode.UPDATE_NAMESRV_CONFIG:
                return this.updateConfig(ctx, request);
            //获取namesrv配置
            case RequestCode.GET_NAMESRV_CONFIG:
                return this.getConfig();
            default:
                String error = " request type " + request.getCode() + " not supported";
                return RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    /**
     * 存储键值对
     */
    public RemotingCommand putKVConfig(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final PutKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(PutKVConfigRequestHeader.class);
        if (requestHeader.getNamespace() == null || requestHeader.getKey() == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("namespace or key is null");
            return response;
        }
        this.namesrvController.getKvConfigManager().putKVConfig(requestHeader.getNamespace(), requestHeader.getKey(), requestHeader.getValue());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取k-v键值对
     */
    public RemotingCommand getKVConfig(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetKVConfigResponseHeader.class);
        final GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.readCustomHeader();
        final GetKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(GetKVConfigRequestHeader.class);
        String value = this.namesrvController.getKvConfigManager().getKVConfig(requestHeader.getNamespace(), requestHeader.getKey());
        if (value != null) {
            responseHeader.setValue(value);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace() + " Key: " + requestHeader.getKey());
        return response;
    }

    /**
     * 删除键值对
     */
    public RemotingCommand deleteKVConfig(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteKVConfigRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteKVConfigRequestHeader.class);
        this.namesrvController.getKvConfigManager().deleteKVConfig(requestHeader.getNamespace(), requestHeader.getKey());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 注册broker
     */
    public RemotingCommand registerBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterBrokerResponseHeader.class);
        final RegisterBrokerResponseHeader responseHeader = (RegisterBrokerResponseHeader) response.readCustomHeader();
        final RegisterBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(RegisterBrokerRequestHeader.class);
        if (!checksum(ctx, request, requestHeader)) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("crc32 not match");
            return response;
        }
        TopicConfigSerializeWrapper topicConfigWrapper;
        List<String> filterServerList = null;
        Version brokerVersion = MQVersion.value2Version(request.getVersion());
        if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
            final RegisterBrokerBody registerBrokerBody = extractRegisterBrokerBodyFromRequest(request, requestHeader);
            topicConfigWrapper = registerBrokerBody.getTopicConfigSerializeWrapper();
            filterServerList = registerBrokerBody.getFilterServerList();
        } else {
            topicConfigWrapper = extractRegisterTopicConfigFromRequest(request);
        }
        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(requestHeader.getClusterName(), requestHeader.getBrokerAddr(), requestHeader.getBrokerName(), requestHeader.getBrokerId(), requestHeader.getHaServerAddr(), request.getExtFields().get(MixAll.ZONE_NAME), requestHeader.getHeartbeatTimeoutMillis(), requestHeader.getEnableActingMaster(), topicConfigWrapper, filterServerList, ctx.channel());
        if (result == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("register broker failed");
            return response;
        }
        responseHeader.setHaServerAddr(result.getHaServerAddr());
        responseHeader.setMasterAddr(result.getMasterAddr());
        if (this.namesrvController.getNamesrvConfig().isReturnOrderTopicConfigToBroker()) {
            byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);
            response.setBody(jsonValue);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private TopicConfigSerializeWrapper extractRegisterTopicConfigFromRequest(final RemotingCommand request) {
        TopicConfigSerializeWrapper topicConfigWrapper;
        if (request.getBody() != null) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestamp(0L);
            topicConfigWrapper.getDataVersion().setStateVersion(0L);
        }
        return topicConfigWrapper;
    }

    private RegisterBrokerBody extractRegisterBrokerBodyFromRequest(RemotingCommand request, RegisterBrokerRequestHeader requestHeader) throws RemotingCommandException {
        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();
        if (request.getBody() != null) {
            try {
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                registerBrokerBody = RegisterBrokerBody.decode(request.getBody(), requestHeader.isCompressed(), brokerVersion);
            } catch (Exception e) {
                throw new RemotingCommandException("Failed to decode RegisterBrokerBody", e);
            }
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestamp(0L);
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setStateVersion(0L);
        }
        return registerBrokerBody;
    }

    /**
     * 获取指定集群/brokerName下的broker列表
     */
    private RemotingCommand getBrokerMemberGroup(RemotingCommand request) throws RemotingCommandException {
        GetBrokerMemberGroupRequestHeader requestHeader = request.decodeCommandCustomHeader(GetBrokerMemberGroupRequestHeader.class);
        BrokerMemberGroup memberGroup = this.namesrvController.getRouteInfoManager().getBrokerMemberGroup(requestHeader.getClusterName(), requestHeader.getBrokerName());
        GetBrokerMemberGroupResponseBody responseBody = new GetBrokerMemberGroupResponseBody();
        responseBody.setBrokerMemberGroup(memberGroup);
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(responseBody.encode());
        return response;
    }

    private boolean checksum(ChannelHandlerContext ctx, RemotingCommand request, RegisterBrokerRequestHeader requestHeader) {
        if (requestHeader.getBodyCrc32() != 0) {
            final int crc32 = UtilAll.crc32(request.getBody());
            if (crc32 != requestHeader.getBodyCrc32()) {
                log.warn(String.format("receive registerBroker request,crc32 not match,from %s",
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel())));
                return false;
            }
        }
        return true;
    }

    public RemotingCommand queryBrokerTopicConfig(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(QueryDataVersionResponseHeader.class);
        final QueryDataVersionResponseHeader responseHeader = (QueryDataVersionResponseHeader) response.readCustomHeader();
        final QueryDataVersionRequestHeader requestHeader = request.decodeCommandCustomHeader(QueryDataVersionRequestHeader.class);
        DataVersion dataVersion = DataVersion.decode(request.getBody(), DataVersion.class);
        String clusterName = requestHeader.getClusterName();
        String brokerAddr = requestHeader.getBrokerAddr();
        Boolean changed = this.namesrvController.getRouteInfoManager().isBrokerTopicConfigChanged(clusterName, brokerAddr, dataVersion);
        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(clusterName, brokerAddr);
        DataVersion nameSeverDataVersion = this.namesrvController.getRouteInfoManager().queryBrokerTopicConfig(clusterName, brokerAddr);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        if (nameSeverDataVersion != null) {
            response.setBody(nameSeverDataVersion.encode());
        }
        responseHeader.setChanged(changed);
        return response;
    }

    /**
     * 注销broker
     */
    public RemotingCommand unregisterBroker(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final UnRegisterBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(UnRegisterBrokerRequestHeader.class);
        if (!this.namesrvController.getRouteInfoManager().submitUnRegisterBrokerRequest(requestHeader)) {
            log.warn("Couldn't submit the unregister broker request to handler, broker info: {}", requestHeader);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * broker心跳检测
     */
    public RemotingCommand brokerHeartbeat(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final BrokerHeartbeatRequestHeader requestHeader = request.decodeCommandCustomHeader(BrokerHeartbeatRequestHeader.class);
        this.namesrvController.getRouteInfoManager().updateBrokerInfoUpdateTimestamp(requestHeader.getClusterName(), requestHeader.getBrokerAddr());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取集群信息
     */
    private RemotingCommand getBrokerClusterInfo() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo().encode();
        response.setBody(content);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand wipeWritePermOfBroker(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(WipeWritePermOfBrokerResponseHeader.class);
        final WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final WipeWritePermOfBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(WipeWritePermOfBrokerRequestHeader.class);
        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(requestHeader.getBrokerName());
        if (ctx != null) {
            log.info("wipe write perm of broker[{}], client: {}, {}", requestHeader.getBrokerName(), RemotingHelper.parseChannelRemoteAddr(ctx.channel()), wipeTopicCnt);
        }
        responseHeader.setWipeTopicCount(wipeTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand addWritePermOfBroker(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(AddWritePermOfBrokerResponseHeader.class);
        final AddWritePermOfBrokerResponseHeader responseHeader = (AddWritePermOfBrokerResponseHeader) response.readCustomHeader();
        final AddWritePermOfBrokerRequestHeader requestHeader = request.decodeCommandCustomHeader(AddWritePermOfBrokerRequestHeader.class);
        int addTopicCnt = this.namesrvController.getRouteInfoManager().addWritePermOfBrokerByLock(requestHeader.getBrokerName());
        responseHeader.setAddTopicCount(addTopicCnt);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 从namesrv获取主题列表
     */
    private RemotingCommand getAllTopicListFromNameserver() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //是否允许获取所有主题列表
        boolean enableAllTopicList = namesrvController.getNamesrvConfig().isEnableAllTopicList();
        if (enableAllTopicList) {
            byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList().encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    /**
     * 往namesrv注册主题
     */
    private RemotingCommand registerTopicToNamesrv(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final RegisterTopicRequestHeader requestHeader = request.decodeCommandCustomHeader(RegisterTopicRequestHeader.class);
        TopicRouteData topicRouteData = TopicRouteData.decode(request.getBody(), TopicRouteData.class);
        if (topicRouteData != null && topicRouteData.getQueueDatas() != null && !topicRouteData.getQueueDatas().isEmpty()) {
            this.namesrvController.getRouteInfoManager().registerTopic(requestHeader.getTopic(), topicRouteData.getQueueDatas());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 删除主题
     */
    private RemotingCommand deleteTopicInNamesrv(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final DeleteTopicFromNamesrvRequestHeader requestHeader = request.decodeCommandCustomHeader(DeleteTopicFromNamesrvRequestHeader.class);
        if (requestHeader.getClusterName() != null && !requestHeader.getClusterName().isEmpty()) {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic(), requestHeader.getClusterName());
        } else {
            this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取namespace下的k-v列表
     */
    private RemotingCommand getKVListByNamespace(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetKVListByNamespaceRequestHeader requestHeader = request.decodeCommandCustomHeader(GetKVListByNamespaceRequestHeader.class);
        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(requestHeader.getNamespace());
        if (null != jsonValue) {
            response.setBody(jsonValue);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("No config item, Namespace: " + requestHeader.getNamespace());
        return response;
    }

    /**
     * 获取集群下的主题列表
     */
    private RemotingCommand getTopicsByCluster(RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //是否允许获取主题列表
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (!enableTopicList) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
            return response;
        }
        final GetTopicsByClusterRequestHeader requestHeader = request.decodeCommandCustomHeader(GetTopicsByClusterRequestHeader.class);
        TopicList topicsByCluster = this.namesrvController.getRouteInfoManager().getTopicsByCluster(requestHeader.getCluster());
        byte[] body = topicsByCluster.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取系统主题列表
     */
    private RemotingCommand getSystemTopicListFromNs() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        TopicList systemTopicList = this.namesrvController.getRouteInfoManager().getSystemTopicList();
        byte[] body = systemTopicList.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private RemotingCommand getUnitTopicList() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList unitTopicList = this.namesrvController.getRouteInfoManager().getUnitTopics();
            byte[] body = unitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand getHasUnitSubTopicList() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList hasUnitSubTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();
            byte[] body = hasUnitSubTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    private RemotingCommand getHasUnitSubUnUnitTopicList() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        boolean enableTopicList = namesrvController.getNamesrvConfig().isEnableTopicList();
        if (enableTopicList) {
            TopicList hasUnitSubUnUnitTopicList = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();
            byte[] body = hasUnitSubUnUnitTopicList.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("disable");
        }
        return response;
    }

    /**
     * 更新namesrv配置
     */
    private RemotingCommand updateConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        if (ctx != null) {
            log.info("updateConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        byte[] body = request.getBody();
        if (body != null) {
            String bodyStr;
            try {
                bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
            } catch (UnsupportedEncodingException e) {
                log.error("updateConfig byte array to string error: ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
            Properties properties = MixAll.string2Properties(bodyStr);
            if (properties == null) {
                log.error("updateConfig MixAll.string2Properties error {}", bodyStr);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("string2Properties error");
                return response;
            }
            if (validateBlackListConfigExist(properties)) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark("Can not update config in black list.");
                return response;
            }
            this.namesrvController.getConfiguration().update(properties);
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    /**
     * 获取namesrv配置
     */
    private RemotingCommand getConfig() {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        String content = this.namesrvController.getConfiguration().getAllConfigsFormatString();
        if (StringUtils.isNotBlank(content)) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("getConfig error, ", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    private boolean validateBlackListConfigExist(Properties properties) {
        for (String blackConfig : configBlackList) {
            if (properties.containsKey(blackConfig)) {
                return true;
            }
        }
        return false;
    }

}