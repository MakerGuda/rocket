package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public interface RemotingClient extends RemotingService {

    /**
     * 更新namesrv地址
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取namesrv列表
     */
    List<String> getNameServerAddressList();

    /**
     * 获取可用的namesrv列表
     */
    List<String> getAvailableNameSrvList();

    /**
     * 同步发送请求
     *
     * @param addr          请求地址
     * @param request       请求体
     * @param timeoutMillis 超时时间
     */
    RemotingCommand invokeSync(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步调用发送请求
     *
     * @param addr           请求地址
     * @param request        请求体
     * @param timeoutMillis  超时时间
     * @param invokeCallback 回调函数
     */
    void invokeAsync(final String addr, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 单向发送
     *
     * @param addr          请求地址
     * @param request       请求体
     * @param timeoutMillis 超时时间
     */
    void invokeOneway(final String addr, final RemotingCommand request, final long timeoutMillis) throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    default CompletableFuture<RemotingCommand> invoke(final String addr, final RemotingCommand request, final long timeoutMillis) {
        CompletableFuture<RemotingCommand> future = new CompletableFuture<>();
        try {
            invokeAsync(addr, request, timeoutMillis, new InvokeCallback() {
                @Override
                public void operationComplete(ResponseFuture responseFuture) {

                }

                @Override
                public void operationSucceed(RemotingCommand response) {
                    future.complete(response);
                }

                @Override
                public void operationFail(Throwable throwable) {
                    future.completeExceptionally(throwable);
                }
            });
        } catch (Throwable t) {
            future.completeExceptionally(t);
        }
        return future;
    }

    void registerProcessor(final int requestCode, final NettyRequestProcessor processor, final ExecutorService executor);

    void setCallbackExecutor(final ExecutorService callbackExecutor);

    boolean isAddressReachable(final String addr);

    void closeChannels(final List<String> addrList);

}