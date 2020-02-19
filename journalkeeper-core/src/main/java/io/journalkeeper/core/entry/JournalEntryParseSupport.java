/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.core.entry;

import io.journalkeeper.core.journal.ParseJournalException;
import io.journalkeeper.utils.parser.EntryParser;

import java.util.LinkedList;
import java.util.List;

/**
 * @author LiYue
 * Date: 2019-01-15
 */
public class JournalEntryParseSupport extends EntryParser {

    public static final int FIXED_LENGTH_8 = 8;
    private final static int VARIABLE_LENGTH = -1;
    private final static int FIXED_LENGTH_1 = 1;
    private final static int FIXED_LENGTH_2 = 2;
    private final static int FIXED_LENGTH_4 = 4;
    private final static List<Attribute> attributeList = new LinkedList<>();
    private static int offset = 0;
    // 第一个变长属性的偏移量
    private static int firstVarOffset = -1;
    // 第一个变长属性在attributes数组中的索引
    private static int firstVarIndex = -1;
    final static int LENGTH = createAttribute("LENGTH", FIXED_LENGTH_4);
    final static int PARTITION = createAttribute("PARTITION", FIXED_LENGTH_2);
    final static int TERM = createAttribute("TERM", FIXED_LENGTH_4);
    final static int MAGIC = createAttribute("MAGIC", FIXED_LENGTH_2);
    final static int BATCH_SIZE = createAttribute("BATCH_SIZE", FIXED_LENGTH_2);
    public final static int TIMESTAMP = createAttribute("TIMESTAMP", FIXED_LENGTH_8);
    final static int ENTRY = createAttribute("ENTRY", VARIABLE_LENGTH);


    public static int getHeaderLength() {
        return firstVarOffset;
    }

    /**
     * 定长消息直接返回offset
     * 变长消息返回属性相对于第一个变长属性的索引值的偏移量的负值：第一个变长属性在attributes中的索引值 - 属性在attributes中的索引值
     * @param length 属性长度，定长消息为正值，负值表示变长消息。
     */
    private static int createAttribute(String name, int length) {

        Attribute attribute = new Attribute(name, length);


        if (attribute.length >= 0) {
            // 定长属性
            if (offset < 0)
                throw new ParseJournalException(
                        "Can not add a fixed length attribute after any variable length attribute!");
            attribute.setOffset(offset);
            offset += length;
        } else {
            // 变长属性
            if (firstVarOffset < 0) { // 第一个变长属性
                firstVarOffset = offset;
                firstVarIndex = attributeList.size();
                offset = -1;
            }
            attribute.setOffset(firstVarIndex - attributeList.size());
        }


        attributeList.add(attribute);
        return attribute.getOffset();
    }

    private static class Attribute {
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

        int getLength() {

            return length;
        }

        int getOffset() {
            return offset;
        }

        void setOffset(int offset) {
            this.offset = offset;
        }

        String getName() {
            return name;
        }
    }


}
