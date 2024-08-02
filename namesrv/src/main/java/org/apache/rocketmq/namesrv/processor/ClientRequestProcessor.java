package org.apache.rocketmq.namesrv.processor;

import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.namesrv.NamesrvUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.namesrv.NamesrvController;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.remoting.protocol.route.TopicRouteData;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClientRequestProcessor implements NettyRequestProcessor {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);
    private final long startupTimeMillis;
    private final AtomicBoolean needCheckNamesrvReady = new AtomicBoolean(true);
    protected NamesrvController namesrvController;

    public ClientRequestProcessor(final NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
        this.startupTimeMillis = System.currentTimeMillis();
    }

    @Override
    public RemotingCommand processRequest(final ChannelHandlerContext ctx, final RemotingCommand request) throws Exception {
        return this.getRouteInfoByTopic(ctx, request);
    }

    /**
     * 通过主题获取主题路由信息
     */
    public RemotingCommand getRouteInfoByTopic(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetRouteInfoRequestHeader requestHeader = request.decodeCommandCustomHeader(GetRouteInfoRequestHeader.class);
        //判断namesrv服务是否启动完毕
        boolean namesrvReady = needCheckNamesrvReady.get() && System.currentTimeMillis() - startupTimeMillis >= TimeUnit.SECONDS.toMillis(namesrvController.getNamesrvConfig().getWaitSecondsForService());
        if (namesrvController.getNamesrvConfig().isNeedWaitForService() && !namesrvReady) {
            log.warn("name server not ready. request code {} ", request.getCode());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("name server not ready");
            return response;
        }

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());
        if (topicRouteData != null) {
            //能成功拉取到主题路由信息，说明服务已经成功启动，此时无需再检查namesrv是否准备就绪
            if (needCheckNamesrvReady.get()) {
                needCheckNamesrvReady.set(false);
            }
            if (this.namesrvController.getNamesrvConfig().isOrderMessageEnable()) {
                String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
                topicRouteData.setOrderTopicConf(orderTopicConf);
            }
            byte[] content;
            Boolean standardJsonOnly = Optional.ofNullable(requestHeader.getAcceptStandardJsonOnly()).orElse(false);
            if (request.getVersion() >= MQVersion.Version.V4_9_4.ordinal() || standardJsonOnly) {
                content = topicRouteData.encode(SerializerFeature.BrowserCompatible, SerializerFeature.QuoteFieldNames, SerializerFeature.SkipTransientField, SerializerFeature.MapSortField);
            } else {
                content = topicRouteData.encode();
            }
            response.setBody(content);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }
        response.setCode(ResponseCode.TOPIC_NOT_EXIST);
        response.setRemark("No topic route info in name server for the topic: " + requestHeader.getTopic() + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        return response;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

}