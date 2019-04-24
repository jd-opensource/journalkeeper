package com.jd.journalkeeper.core.journal;

import com.jd.journalkeeper.utils.parser.EntryParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

/**
 * @author liyue25
 * Date: 2019-01-15
 */
public class StorageEntryParser extends EntryParser {

    public final static int VARIABLE_LENGTH = -1;
    public final static int FIXED_LENGTH_1 = 1;
    public final static int FIXED_LENGTH_2 = 2;
    public final static int FIXED_LENGTH_4 = 4;
    private static int offset = 0;
    // 第一个变长属性的偏移量
    private static int firstVarOffset = -1;
    // 第一个变长属性在attributes数组中的索引
    private static int firstVarIndex = -1;
    private final static List<Attribute> attributeList= new LinkedList<>();

    public final static int LENGTH = createAttribute("LENGTH",FIXED_LENGTH_4);
    public final static int MAGIC = createAttribute("MAGIC",FIXED_LENGTH_2);
    public final static int TERM = createAttribute("TERM",FIXED_LENGTH_4);
    public final static int TYPE = createAttribute("TYPE",FIXED_LENGTH_1);
    public final static int ENTRY = createAttribute("ENTRY",VARIABLE_LENGTH);


    private final static Attribute [] attributes = attributeList.toArray(new Attribute[0]);

    public static int getHeaderLength(){return firstVarOffset;}

    public static ByteBuffer getByteBuffer(ByteBuffer messageBuffer, int relativeOffset) {
        int offset = firstVarOffset;
        // 计算偏移量

        int length;
        for(int index = firstVarIndex; index < firstVarIndex - relativeOffset; index++) {
            offset += getInt(messageBuffer, LENGTH) - getHeaderLength();
        }

        length = getInt(messageBuffer, LENGTH) - getHeaderLength();
        if(length < 0) throw new ParseJournalException("Invalid offset: " + relativeOffset);

        ByteBuffer byteBuffer = messageBuffer.slice();
        byteBuffer.position(offset);
        byteBuffer.limit(offset + length);
        return byteBuffer;

    }

    public static byte [] getBytes(ByteBuffer messageBuffer, int relativeOffset) {
        ByteBuffer byteBuffer = getByteBuffer(messageBuffer, relativeOffset);

        ByteBuffer arrayBuffer = ByteBuffer.allocate(byteBuffer.remaining());
        arrayBuffer.put(byteBuffer);
        return arrayBuffer.array();
    }

    public static String getString(ByteBuffer messageBuffer) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Attribute attribute : attributes) {
            stringBuilder.append(attribute.getName()).append("(");
            if (attribute.getLength() >= 0) {
                stringBuilder.append(attribute.getLength()).append("): ");
            }
            switch (attribute.getLength()) {
                case FIXED_LENGTH_2:
                    try {
                        short s = getShort(messageBuffer, attribute.getOffset());
                        stringBuilder.append(s).append("\n");
                    } catch (Exception e) {
                        stringBuilder.append("Exception:").append(e).append("\n");
                    }
                    break;
                case FIXED_LENGTH_4:
                    try {
                        int ii = getInt(messageBuffer, attribute.getOffset());
                        stringBuilder.append(ii);
                        stringBuilder.append("\n");
                    } catch (Exception e) {
                        stringBuilder.append("Exception:").append(e).append("\n");
                    }
                    break;
                default:
                    appendBytes(messageBuffer, stringBuilder, attribute);
            }


        }


        return stringBuilder.toString();
    }

    private static void appendBytes(ByteBuffer messageBuffer, StringBuilder stringBuilder, Attribute attribute) {
        try {
            byte [] bytes;

            if(attribute.getLength() < 0) {
                bytes = getBytes(messageBuffer, attribute.getOffset());
                stringBuilder.append(bytes.length);
                stringBuilder.append("): ");
            } else {
                bytes = getBytes(messageBuffer, attribute);
            }
            stringBuilder.append("\n\tHex: ");
            for (int j = 0; j < bytes.length && j < 32; j++) {
                stringBuilder.append(String.format("0x%02X ", bytes[j]));
            }
            if (bytes.length > 32) stringBuilder.append("...");
            stringBuilder.append("\n\tString: ").append(new String(bytes, StandardCharsets.UTF_8));

        } catch (Exception e) {
            stringBuilder.append("Exception:").append(e);
        }
        stringBuilder.append("\n");

    }

    private static byte[] getBytes(ByteBuffer messageBuffer, Attribute attribute) {
        byte[] bytes;
        ByteBuffer byteBuffer = messageBuffer.slice();
        byteBuffer.position(byteBuffer.position() + attribute.getOffset());
        byteBuffer.limit(byteBuffer.position() + attribute.getLength());

        ByteBuffer buffer = ByteBuffer.allocate(attribute.length);
        buffer.put(byteBuffer);
        buffer.flip();
        bytes = buffer.array();
        return bytes;
    }


    /**
     * 定长消息直接返回offset
     * 变长消息返回属性相对于第一个变长属性的索引值的偏移量的负值：第一个变长属性在attributes中的索引值 - 属性在attributes中的索引值
     * @param length 属性长度，定长消息为正值，负值表示变长消息。
     */
    private static int createAttribute(String name, int length){

        Attribute attribute = new Attribute(name, length);


        if(attribute.length >= 0) {
            // 定长属性
            if(offset < 0)
                throw new ParseJournalException(
                        "Can not add a fixed length attribute after any variable length attribute!");
            attribute.setOffset(offset);
            offset += length;
        } else {
            // 变长属性
            if(firstVarOffset < 0 ) { // 第一个变长属性
                firstVarOffset = offset;
                firstVarIndex = attributeList.size();
                offset = -1;
            }
            attribute.setOffset(firstVarIndex - attributeList.size());
        }


        attributeList.add(attribute);
        return attribute.getOffset();
    }

    static class Attribute {
        private final int length;
        private final String name;
        private int offset = -1;

        /**
         * 定长属性
         * @param length 长度
         */
        Attribute(String name, int length) {
            this.name = name;
            this.length = length;
        }

        public int getLength() {

            return length;
        }

        public int getOffset() {
            return offset;
        }

        public void setOffset(int offset) {
            this.offset = offset;
        }

        public String getName() {
            return name;
        }
    }

    public static StorageEntry parseHeader(ByteBuffer headerBuffer) {
        StorageEntry storageEntry = new StorageEntry();
        checkMagic(headerBuffer);
        storageEntry.setLength(getInt(headerBuffer, LENGTH));
        storageEntry.setTerm(getInt(headerBuffer, TERM));
        storageEntry.setType(getByte(headerBuffer, TYPE));
        return storageEntry;
    }

    private static void checkMagic(ByteBuffer headerBuffer) {
        if (StorageEntry.MAGIC != getShort(headerBuffer,StorageEntryParser.MAGIC)) {
            throw new ParseJournalException("Check magic failed！");
        }
    }

    public static  void serialize(ByteBuffer destBuffer, StorageEntry storageEntry) {

        destBuffer.putInt(storageEntry.getLength());
        destBuffer.putShort(StorageEntry.MAGIC);
        destBuffer.putInt(storageEntry.getTerm());
        destBuffer.put(storageEntry.getType());
        destBuffer.put(storageEntry.getEntry());
    }
}
