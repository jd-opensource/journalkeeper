package com.jd.journalkeeper.core.api;

/**
 * 定义返回响应的级别：
 * ONE_WAY: 客户端单向向Server发送请求，无应答直接返回；
 * RECEIVE: Server收到请求后应答；
 * PERSISTENCE：Server将消息写入磁盘后应答；
 * REPLICATION：Server将消息复制到集群大多数节点后应答，默认值；
 *
 * @author liyue25
 * Date: 2019-04-23
 */
public enum ResponseConfig {
    ONE_WAY(0),
    RECEIVE(1),
    PERSISTENCE(2),
    REPLICATION(3);

    private int value;

    ResponseConfig(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static ResponseConfig valueOf(final int value) {
        switch (value) {
            case 0:
                return ONE_WAY;
            case 1:
                return RECEIVE;
            case 2:
                return PERSISTENCE;
            default:
                return REPLICATION;
        }
    }
}
