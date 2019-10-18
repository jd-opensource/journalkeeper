/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.journalkeeper.sql.client.domain;

/**
 * OperationTypes
 * author: gaohaoxiang
 * date: 2019/5/30
 */
public enum OperationTypes {

    INSERT(0),

    UPDATE(1),

    DELETE(2),

    QUERY(3),

    BATCH(4),

    ;

    private int type;

    OperationTypes(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static OperationTypes valueOf(int type) {
        switch (type) {
            case 0:
                return INSERT;
            case 1:
                return UPDATE;
            case 2:
                return DELETE;
            case 3:
                return QUERY;
            case 4:
                return BATCH;
            default:
                throw new UnsupportedOperationException(String.valueOf(type));
        }
    }
}