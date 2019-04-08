package com.jd.journalkeeper.utils.net;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author liyue25
 * Date: 2019-04-08
 */
public class NetworkingUtils {
    public static int findRandomOpenPortOnAllLocalInterfaces() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
