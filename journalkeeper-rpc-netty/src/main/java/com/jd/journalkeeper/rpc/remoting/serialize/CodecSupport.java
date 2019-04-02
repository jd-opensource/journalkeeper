package com.jd.journalkeeper.rpc.remoting.serialize;

import com.jd.journalkeeper.rpc.remoting.transport.codec.Decoder;
import com.jd.journalkeeper.rpc.remoting.transport.codec.Encoder;
import io.netty.buffer.ByteBuf;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-03-28
 */
public class CodecSupport {
    public static String decodeString(ByteBuf byteBuf){
        return new String(decodeBytes(byteBuf), StandardCharsets.UTF_8);
    }

    public static void encodeString(ByteBuf byteBuf, String str) {
        byte [] bytes = str == null ? new byte[0] : str.getBytes(StandardCharsets.UTF_8);
        encodeBytes(byteBuf, bytes);
    }

    public static byte [] decodeBytes(ByteBuf byteBuf){
        int length = byteBuf.readInt();
        if(length > 0) {
            byte [] buff = new byte[length];
            byteBuf.readBytes(buff);
            return buff;
        }
        return new byte[0];
    }

    public static void encodeBytes(ByteBuf byteBuf, byte [] bytes) {
        if(null == bytes) {
            bytes = new byte[0];
        }
        byteBuf.writeInt(bytes.length);
        if(bytes.length > 0) {
            byteBuf.writeBytes(bytes);
        }
    }


    public static void encodeLong(ByteBuf byteBuf, long l) {
        byteBuf.writeLong(l);
    }

    public static long decodeLong(ByteBuf byteBuf) {
        return byteBuf.readLong();
    }
    public static void encodeInt(ByteBuf byteBuf, int i) {
        byteBuf.writeInt(i);
    }

    public static int decodeInt(ByteBuf byteBuf) {
        return byteBuf.readInt();
    }

    public static <T> void encodeList(ByteBuf byteBuf, List<T> list, Encoder itemEncoder) {
        if(null == list) list = Collections.emptyList();
        byteBuf.writeInt(list.size());
        list.forEach(item -> itemEncoder.encode(item , byteBuf));
    }

    public static <T> List<T> decodeList(ByteBuf byteBuf, Decoder itemDecoder) {
        int size = byteBuf.readInt();
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add((T) itemDecoder.decode(byteBuf));
        }
        return list;
    }

    public static void encodeUri(ByteBuf byteBuf, URI uri) {
        encodeString(byteBuf, null == uri ? null : uri.toString());
    }

    public static URI decodeUri(ByteBuf byteBuf) {
        String uriString = decodeString(byteBuf);
        if(uriString.isEmpty()) return null;
        else return URI.create(uriString);
    }

    public static void encodeBoolean(ByteBuf byteBuf, boolean bool) {
        byteBuf.writeByte(bool ? 0X1 : 0X0);
    }

    public static boolean decodeBoolean(ByteBuf byteBuf) {
        return byteBuf.readByte() == 0X1;
    }
}
