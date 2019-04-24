package com.jd.journalkeeper.utils.parser;

import java.nio.ByteBuffer;

/**
 * @author liyue25
 * Date: 2019-04-24
 */
public class EntryParser {
    public static byte getByte(ByteBuffer messageBuffer, int offset){
        return messageBuffer.get(messageBuffer.position() + offset);
    }

    public static void setByte(ByteBuffer messageBuffer, int offset, byte value){
        messageBuffer.put(messageBuffer.position() + offset,value);
    }

    public static short getShort(ByteBuffer messageBuffer, int offset){
        return messageBuffer.getShort(messageBuffer.position() + offset);
    }

    public static void setShort(ByteBuffer messageBuffer, int offset, short value){
        messageBuffer.putShort(messageBuffer.position() + offset,value);
    }

    public static int getBit(ByteBuffer messageBuffer, int byteOffset, int bitOffset){
        byte b = getByte(messageBuffer, byteOffset);
        return (b >> bitOffset) & 1;
    }

    public static void setBit(ByteBuffer messageBuffer, int byteOffset, int bitOffset, boolean bitValue){

        byte b = getByte(messageBuffer, byteOffset);
        if(bitValue) {
            b |= 1 << bitOffset;
        } else {
            b &= ~(1 << bitOffset);
        }
        setByte(messageBuffer, byteOffset, b);
    }

    public static int getInt(ByteBuffer messageBuffer, int offset){
        return messageBuffer.getInt(messageBuffer.position() + offset);
    }

    public static void setInt(ByteBuffer messageBuffer, int offset, int value){
        messageBuffer.putInt(messageBuffer.position() + offset,value);
    }

    public static long getLong(ByteBuffer messageBuffer, int offset){
        return messageBuffer.getLong(messageBuffer.position() + offset);
    }

    public static void setLong(ByteBuffer messageBuffer, int offset, long value){
        messageBuffer.putLong(messageBuffer.position() + offset,value);
    }
}
