package com.jd.journalkeeper.coordinating.server.config;

/**
 * CoordinatingConfigs
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingConfigs {

    public static final String SERVER_PORT = "coordinating.server.port";
    public static final int DEFAULT_SERVER_PORT = 50081;

    public static final String SERVER_HOST = "coordinating.server.host";
    public static final String SERVER_ACCEPT_THREAD = "coordinating.server.acceptThread";
    public static final String SERVER_IO_THREAD = "coordinating.server.ioThread";
    public static final String SERVER_MAX_IDLE_TIME = "coordinating.server.maxIdleTime";
    public static final String SERVER_REUSE_ADDRESS = "coordinating.server.reuseAddress";
    public static final String SERVER_SO_LINGER = "coordinating.server.soLinger";
    public static final String SERVER_TCP_NO_DELAY = "coordinating.server.tcpNoDelay";
    public static final String SERVER_KEEPALIVE = "coordinating.server.keepAlive";
    public static final String SERVER_SO_TIMEOUT = "coordinating.server.soTimeout";
    public static final String SERVER_SOCKET_BUFFER_SIZE = "coordinating.server.socketBufferSize";
    public static final String SERVER_FRAME_MAX_SIZE = "coordinating.server.frameMaxSize";
    public static final String SERVER_BACKLOG = "coordinating.server.backlog";
    public static final String SERVER_MAX_ONEWAY = "coordinating.server.maxOneway";
    public static final String SERVER_NONBLOCK_ONEWAY = "coordinating.server.nonBlockOneway";
    public static final String SERVER_MAX_ASYNC = "coordinating.server.maxAsync";
    public static final String SERVER_CALLBACK_THREADS = "coordinating.server.callbackThreads";
    public static final String SERVER_SEND_TIMEOUT = "coordinating.server.sendTimeout";
    public static final String SERVER_MAX_RETRYS = "coordinating.server.maxRetrys";
    public static final String SERVER_MAX_RETRY_DELAY = "coordinating.server.maxRetryDelay";
    public static final String SERVER_RETRY_DELAY = "coordinating.server.retryDelay";

    public static final String SERVER_EXECUTOR_THREADS = "coordinating.server.executor.threads";
    public static final int DEFAULT_SERVER_EXECUTOR_THREADS = Runtime.getRuntime().availableProcessors() * 3;
    public static final String SERVER_EXECUTOR_QUEUE_SIZE = "coordinating.server.executor.queue.size";
    public static final int DEFAULT_SERVER_EXECUTOR_QUEUE_SIZE = 1024;
}