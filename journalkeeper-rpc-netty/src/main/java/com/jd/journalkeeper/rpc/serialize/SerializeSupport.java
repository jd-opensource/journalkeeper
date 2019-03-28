package com.jd.journalkeeper.rpc.serialize;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class SerializeSupport {
    public static String readString(ByteBuf byteBuf){
        int length = byteBuf.readInt();
        if(length > 0) {
            byte [] buff = new byte[length];
            byteBuf.readBytes(buff);
            return new String(buff, StandardCharsets.UTF_8);
        }
        return null;
    }

    public static void writeString(ByteBuf byteBuf, String str) {
        byte [] bytes = str == null ? new byte[0] : str.getBytes(StandardCharsets.UTF_8);
        byteBuf.writeInt(bytes.length);
        if(bytes.length > 0) {
            byteBuf.writeBytes(bytes);
        }
    }
}
