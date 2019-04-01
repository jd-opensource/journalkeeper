package com.jd.journalkeeper.rpc.utils;

import java.net.*;

/**
 * @author liyue25
 * Date: 2019-04-01
 */
public class UriUtils {
    private final static String SCHEMA = "jk";
    public static SocketAddress toSockAddress(URI uri) {
       return new InetSocketAddress(uri.getHost(), uri.getPort());
    }
}
