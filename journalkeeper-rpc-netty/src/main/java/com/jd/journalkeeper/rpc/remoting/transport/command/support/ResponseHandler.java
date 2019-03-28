package com.jd.journalkeeper.rpc.remoting.transport.command.support;

import com.jd.journalkeeper.rpc.remoting.transport.RequestBarrier;
import com.jd.journalkeeper.rpc.remoting.transport.ResponseFuture;
import com.jd.journalkeeper.rpc.remoting.transport.Transport;
import com.jd.journalkeeper.rpc.remoting.transport.command.Command;
import com.jd.journalkeeper.rpc.remoting.transport.command.Header;
import com.jd.journalkeeper.rpc.remoting.transport.command.handler.ExceptionHandler;
import com.jd.journalkeeper.rpc.remoting.transport.config.TransportConfig;
import com.jd.journalkeeper.utils.threads.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 响应处理器
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2018/8/24
 */
public class ResponseHandler {

    protected static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

    private TransportConfig config;
    private RequestBarrier barrier;
    private ExceptionHandler exceptionHandler;
    private ExecutorService asyncExecutorService;

    public ResponseHandler(TransportConfig transportConfig, RequestBarrier barrier, ExceptionHandler exceptionHandler) {
        this.config = transportConfig;
        this.barrier = barrier;
        this.exceptionHandler = exceptionHandler;
        this.asyncExecutorService = newAsyncExecutorService();
    }

    public void handle(Transport transport, Command command) {
        Header header = command.getHeader();
        // 超时被删除了
        final ResponseFuture responseFuture = barrier.get(header.getRequestId());
        if (responseFuture == null) {
            if (logger.isInfoEnabled()) {
                logger.info(String.format("request is timeout %s", header));
            }
            return;
        }
        // 设置应答
        responseFuture.setResponse(command);
        // 异步调用
        if (responseFuture.getCallback() != null) {
            boolean success = false;
            ExecutorService executor = this.asyncExecutorService;
            if (executor != null) {
                try {
                    executor.execute(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                responseFuture.onSuccess();
                            } catch (Throwable e) {
                                logger.error("execute callback error.", e);
                            } finally {
                                responseFuture.release();
                            }
                        }
                    });
                    success = true;
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                }
            }

            if (!success) {
                try {
                    responseFuture.onSuccess();
                } catch (Throwable e) {
                    logger.error("execute callback error.", e);
                } finally {
                    responseFuture.release();
                }
            }
        } else {
            // 释放资源，不回调
            if (!responseFuture.release()) {
                // 已经被释放了
                return;
            }
        }
        barrier.remove(header.getRequestId());
    }

    protected ExecutorService newAsyncExecutorService() {
        return Executors.newFixedThreadPool(config.getCallbackThreads(), new NamedThreadFactory("JournalKeeper-Async-Callback"));
    }
}