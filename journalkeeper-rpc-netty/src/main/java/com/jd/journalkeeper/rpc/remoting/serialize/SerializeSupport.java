package com.jd.journalkeeper.rpc.remoting.serialize;

import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class SerializeSupport {
    public static String readString(ByteBuf byteBuf){
        return new String(readBytes(byteBuf), StandardCharsets.UTF_8);
    }

    public static void writeString(ByteBuf byteBuf, String str) {
        byte [] bytes = str == null ? new byte[0] : str.getBytes(StandardCharsets.UTF_8);
        writeBytes(byteBuf, bytes);
    }

    public static byte [] readBytes(ByteBuf byteBuf){
        int length = byteBuf.readInt();
        if(length > 0) {
            byte [] buff = new byte[length];
            byteBuf.readBytes(buff);
            return buff;
        }
        return new byte[0];
    }

    public static void writeBytes(ByteBuf byteBuf, byte [] bytes) {
        if(null == bytes) {
            bytes = new byte[0];
        }
        byteBuf.writeInt(bytes.length);
        if(bytes.length > 0) {
            byteBuf.writeBytes(bytes);
        }
    }



}
