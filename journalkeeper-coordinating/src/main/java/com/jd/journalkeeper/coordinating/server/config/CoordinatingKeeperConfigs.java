package com.jd.journalkeeper.coordinating.server.config;

import com.jd.journalkeeper.rpc.remoting.transport.IpUtil;

/**
 * CoordinatingKeeperConfigs
 * author: gaohaoxiang
 * email: gaohaoxiang@jd.com
 * date: 2019/6/3
 */
public class CoordinatingKeeperConfigs {

    public static final String HOST = "coordinating.keeper.host";
    public static final String DEFAULT_HOST = IpUtil.getLocalIp();
    public static final String PORT = "coordinating.keeper.port";
    public static final int DEFAULT_PORT = 50082;

    public static final String CLUSTER = "coordinating.keeper.cluster";
    public static final String CLUSTER_SEPARATOR = ";";
    public static final String CLUSTER_PORT_SEPARATOR = ":";

    public static final String ROLE = "coordinating.keeper.role";
}